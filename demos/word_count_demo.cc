/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2023 ScyllaDB Ltd.
 */

#include <iostream>
#include <memory>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/log.hh>

namespace {
seastar::logger logger("word_count");

static constexpr size_t kBufferSize = 4096;

using WordCountMap = std::unordered_map<std::string, size_t>;
using WordCountMapPtr = std::unique_ptr<WordCountMap>;
using ShouldReadBufferFunc = std::function<bool(const char *, size_t)>;

class FileMapReduceTask {
public:
  virtual ~FileMapReduceTask() = default;
  virtual void map(const char &ch) = 0;
  virtual void onMapComplete() = 0;
  virtual void reduce(const FileMapReduceTask &other) = 0;
};

using FileMapReduceTaskPtr = std::unique_ptr<FileMapReduceTask>;
using FileMapReduceTaskFactoryFunc = std::function<FileMapReduceTaskPtr()>;

class WordCountTask : public FileMapReduceTask {
public:
  void map(const char &ch) final {
    static constexpr const char *kDelimiters = " \n.,:;";
    if (std::strchr(kDelimiters, ch) == nullptr) {
      _word += ch;
      return;
    }

    if (!_word.empty()) {
      (*_wordCountMap)[_word]++;
      _word.clear();
    }
  }

  void onMapComplete() final {
    logger.info("Total words: {}", _wordCountMap->size());
  }

  void reduce(const FileMapReduceTask &otherFileMapReduceTask) final {
    const auto &otherWordCountMap =
        static_cast<const WordCountTask &>(otherFileMapReduceTask)
            ._wordCountMap;

    for (const auto &[word, count] : *otherWordCountMap) {
      (*_wordCountMap)[word] += count;
    }
  }

  static FileMapReduceTaskPtr create() {
    return std::make_unique<WordCountTask>();
  }

private:
  std::string _word;
  WordCountMapPtr _wordCountMap = std::make_unique<WordCountMap>();
};

class FileMapReduceProcessor {
public:
  FileMapReduceProcessor(seastar::file &&fileDesc, size_t startOffset,
                         size_t estimatedBytesToRead,
                         FileMapReduceTaskPtr &&mapReduceTask)
      : _fileDesc{std::move(fileDesc)}, _fileOffset{startOffset},
        _estimatedBytesToRead{estimatedBytesToRead}, _mapReduceTask{std::move(
                                                         mapReduceTask)} {}

  seastar::future<FileMapReduceTaskPtr> process() {
    return seastar::repeat([this] {
             return seastar::do_with(
                 seastar::allocate_aligned_buffer<char>(kBufferSize,
                                                        kBufferSize),
                 [this](auto &buffer) {
                   return _fileDesc
                       .dma_read(_fileOffset, buffer.get(), kBufferSize)
                       .then([this, &buffer](size_t bytesRead) {
                         return _processBuffer(buffer.get(), bytesRead)
                                    ? seastar::stop_iteration::yes
                                    : seastar::stop_iteration::no;
                       });
                 });
           })
        .then([this] {
          return seastar::make_ready_future<FileMapReduceTaskPtr>(
              std::move(_mapReduceTask));
        });
  }

private:
  bool _processBuffer(const char *buffer, const size_t &bufferSize) {
    if (bufferSize == 0) {
      return true;
    }

    auto isComplete = false;
    auto bufferOffset = _getBufferStartOffset(buffer, bufferSize);

    if (_totalBytesRead < _estimatedBytesToRead) {
      std::tie(isComplete, bufferOffset) =
          _populateWordCount(buffer, bufferSize, bufferOffset);
    }

    if (!isComplete && _totalBytesRead >= _estimatedBytesToRead) {
      std::tie(isComplete, bufferOffset) =
          _populateRemaining(buffer, bufferSize, bufferOffset);
    }

    _fileOffset += kBufferSize;
    return isComplete;
  }

  size_t _getBufferStartOffset(const char *buffer, const size_t &bufferSize) {
    if (_fileOffset == 0 || _totalBytesRead > 0) {
      return 0;
    }

    const char *notFound = buffer + bufferSize;
    const char *result = std::find(buffer, buffer + bufferSize, '\n');
    auto offset = result == notFound ? bufferSize : result - buffer + 1;
    _totalBytesRead += offset;
    return offset;
  }

  std::pair<bool, size_t> _populateWordCount(const char *buffer,
                                             const size_t &bufferSize,
                                             size_t offset) {
    char ch = '\0';
    for (; offset < bufferSize && _totalBytesRead < _estimatedBytesToRead;
         offset++) {
      ch = buffer[offset];
      _executeMap(ch);
    }

    return {ch == '\n', offset};
  }

  std::pair<bool, size_t> _populateRemaining(const char *buffer,
                                             const size_t &bufferSize,
                                             size_t offset) {
    bool isComplete = false;
    for (; offset < bufferSize; offset++) {
      const char ch = buffer[offset];
      _executeMap(ch);

      if (ch == '\n') {
        isComplete = true;
        break;
      }
    }

    return {isComplete, offset};
  }

  void _executeMap(const char &ch) {
    _totalBytesRead++;
    _mapReduceTask->map(ch);
  }

public:
  seastar::file _fileDesc;

  size_t _fileOffset;
  const size_t _estimatedBytesToRead;
  size_t _totalBytesRead = 0;

  FileMapReduceTaskPtr _mapReduceTask;
};

class FileMapReduce {
public:
  FileMapReduce(FileMapReduceTaskFactoryFunc &&mapReduceTaskFactoryFunc)
      : _mapReduceTaskFactoryFunc{std::move(mapReduceTaskFactoryFunc)} {
    _app.add_options()("file_path",
                       boost::program_options::value<std::string>()->required(),
                       "Path to file to count words in");
  }

  int run(int argc, char **argv) {
    return _app.run(argc, argv, [this] {
      auto &&config = _app.configuration();
      const auto filePath = config[kFilePathKey].as<std::string>();

      return seastar::file_stat(filePath).then(
          [this, filePath](seastar::stat_data &&stat) -> seastar::future<> {
            return _mapFileRegionToCores(filePath, stat.size);
          });
    });
  }

private:
  seastar::future<> _mapFileRegionToCores(const std::string &filePath,
                                          const size_t &fileSize) {
    size_t chunkSize = fileSize / seastar::smp::count;
    chunkSize = chunkSize / kBufferSize * kBufferSize;
    chunkSize = chunkSize > kBufferSize ? chunkSize : kBufferSize;
    return seastar::do_with(
        filePath, fileSize, chunkSize,
        [this](auto &filePath, auto &fileSize, auto &chunkSize) {
          return seastar::smp::invoke_on_all(
              [this, &filePath, &fileSize, &chunkSize] {
                const auto coreId = seastar::this_shard_id();
                const size_t offset = coreId * chunkSize;
                const size_t bytesToRead =
                    coreId != seastar::smp::count - 1
                        ? chunkSize
                        : fileSize - (seastar::smp::count - 2) * chunkSize;
                return _openFileRegionInCore(filePath, fileSize, offset,
                                             bytesToRead);
              });
        });
  }

  seastar::future<> _openFileRegionInCore(const std::string &filePath,
                                          const size_t &fileSize, size_t offset,
                                          size_t bytesToRead) {
    return seastar::open_file_dma(filePath, seastar::open_flags::ro)
        .then([this, fileSize, offset, bytesToRead](seastar::file fileDesc) {
          auto fileProcessor =
              FileMapReduceProcessor(std::move(fileDesc), offset, bytesToRead,
                                     _mapReduceTaskFactoryFunc());
          return seastar::do_with(
              std::move(fileProcessor), [](auto &fileProcessor) {
                return fileProcessor.process().then([](auto &&mapReduceTask) {
                  mapReduceTask->onMapComplete();
                });
              });
        });
  }

  static constexpr const char *kFilePathKey = "file_path";
  seastar::app_template _app;

  FileMapReduceTaskFactoryFunc _mapReduceTaskFactoryFunc;
};

} // namespace

int main(int argc, char **argv) {
  seastar::app_template app;
  FileMapReduce processor(WordCountTask::create);

  // return app.run(argc, argv, [] {
  //   std::vector<int> chunks = {1, 2, 3, 4};
  //   auto mapper = [](auto &c) { return seastar::make_ready_future<int>(c); };
  //   return seastar::map_reduce(
  //              chunks.begin(), chunks.end(), mapper, 0,
  //              [](const int &c1, const int &c2) { return c1 + c2; })
  //       .then([](auto result) {
  //         std::cout << result << std::endl;
  //         return seastar::make_ready_future<>();
  //       });
  // });

  return processor.run(argc, argv);
}
