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

class WordCount {
public:
  WordCount(seastar::file fileDesc, size_t startOffset,
            size_t estimatedBytesToRead)
      : _fileDesc{std::move(fileDesc)}, _offset{startOffset},
        _estimatedBytesToRead{estimatedBytesToRead} {}

  seastar::future<WordCountMapPtr> process() {
    return seastar::repeat([this] {
             return seastar::do_with(
                 seastar::allocate_aligned_buffer<char>(kBufferSize,
                                                        kBufferSize),
                 [this](auto &buffer) {
                   return _fileDesc.dma_read(_offset, buffer.get(), kBufferSize)
                       .then([this, &buffer](size_t bytesRead) {
                         logger.info("Read {} bytes", bytesRead);
                         if (bytesRead == 0) {
                           return seastar::stop_iteration::yes;
                         }

                         _offset = _getStartOffset(buffer.get(), bytesRead);
                         if (_offset < _estimatedBytesToRead) {
                           std::tie(_isComplete, _offset) = _populateWordCount(
                               buffer.get(), bytesRead, _offset);
                         }

                         if (!_isComplete && _offset >= _estimatedBytesToRead) {
                           std::tie(_isComplete, _offset) = _populateRemaining(
                               buffer.get(), bytesRead, _offset);
                         }

                         return _isComplete ? seastar::stop_iteration::yes
                                            : seastar::stop_iteration::no;
                       });
                 });
           })
        .then([this] {
          return seastar::make_ready_future<WordCountMapPtr>(
              std::move(_wordCountMap));
        });
  }

private:
  size_t _getStartOffset(const char *buffer, const size_t &bufferSize) {
    size_t idx = 0;
    if (buffer[idx] != '\n' || _totalBytesRead > 0) {
      return idx;
    }

    const char *notFound = buffer + bufferSize;
    const char *result = std::find(buffer + idx + 1, buffer + bufferSize, '\n');
    return result == notFound ? bufferSize : result - buffer + 1;
  }

  std::pair<bool, size_t> _populateWordCount(const char *buffer,
                                             const size_t &bufferSize,
                                             size_t offset) {
    for (; offset < bufferSize && _totalBytesRead < _estimatedBytesToRead;
         offset++) {
      const char ch = buffer[offset];
      _maybeInsertWord(ch, _word);
    }

    return {_word.empty(), offset};
  }

  std::pair<bool, size_t> _populateRemaining(const char *buffer,
                                             const size_t &bufferSize,
                                             size_t offset) {
    bool isComplete = false;
    for (; offset < bufferSize; offset++) {
      const char ch = buffer[offset];
      _maybeInsertWord(ch, _word);

      if (ch == '\n') {
        isComplete = true;
        break;
      }
    }

    return {isComplete, offset};
  }

  void _maybeInsertWord(const char &ch, std::string &word) {
    _totalBytesRead++;

    if (ch != ' ' && ch != '\n') {
      word += ch;
      return;
    }

    if (!word.empty()) {
      (*_wordCountMap)[word]++;
      word.clear();
    }
  }

  seastar::file _fileDesc;

  size_t _offset;
  const size_t _estimatedBytesToRead;
  size_t _totalBytesRead = 0;
  bool _isComplete = false;

  std::string _word;

  WordCountMapPtr _wordCountMap = std::make_unique<WordCountMap>();
};

// class ParallelFileProcessJob {
// public:
// };

// class ParallelFileProcessor {
// public:
//   ParallelFileProcessor() {
//     _app.add_options()("file_path",
//                        boost::program_options::value<std::string>()->required(),
//                        "Path to file to count words in");
//   }

//   int run(int argc, char **argv) {
//     return _app.run(argc, argv, [this] {
//       auto &&config = _app.configuration();
//       filePath = config[kFilePathKey].as<std::string>();

//       seastar::file_stat(filePath).then([this](seastar::stat_data &&stat) {
//         _mapFileToCores(filePath, stat.size);
//       });
//       return seastar::open_file_dma(_filePath, seastar::open_flags::ro)
//           .then([](seastar::file fileDesc) {
//             // const auto fileSize = fileDesc.size().get0();
//             return seastar::do_with(
//                 WordCount(std::move(fileDesc), 2, 17),
//                 [](WordCount &wordCount) {
//                   return wordCount.process().then([](WordCountMapPtr map) {
//                     for (const auto &pair : *map) {
//                       std::cout << pair.first << " " << pair.second
//                                 << std::endl;
//                     }
//                   });
//                 });
//           });
//     });
//   }

// private:
//   static constexpr const char *kFilePathKey = "file_path";

//   void _mapFileRegionToCores(const std::string &filePath, const size_t &fileSize) {
//     size_t chunkSize = fileSize / seastar::smp::count;
//     chunkSize = chunkSize / kBufferSize * kBufferSize;
//     chunkSize = chunkSize > kBufferSize ? chunkSize : kBufferSize;

//     seastar::smp::invoke_on_all([this, filePath, chunkSize, fileSize](auto coreId) {
//       const size_t offset = coreId * chunkSize;
//       const size_t bytesToRead =
//           coreId != seastar::smp::count - 1 ? chunkSize : fileSize - offset;


//     });
//   }

//   void _openFileRegionInCore(const std::string&filePath, const size_t& fileSize, size_t offset, size_t chunkSize)   {

//   }

//   seastar::app_template _app;
// };

} // namespace

int main(int argc, char **argv) {
  seastar::app_template app;
  app.add_options()("file_path",
                    boost::program_options::value<std::string>()->required(),
                    "Path to file to count words in");

  return app.run(argc, argv, [&app] {
    auto &&config = app.configuration();
    auto filePath = config["file_path"].as<std::string>();
    return seastar::open_file_dma(filePath, seastar::open_flags::ro)
        .then([](seastar::file fileDesc) {
          const auto fileSize = fileDesc.size().get0();
          return seastar::do_with(
              WordCount(std::move(fileDesc), 0, fileSize), [](WordCount &wordCount) {
                return wordCount.process().then([](WordCountMapPtr map) {
                  for (const auto &pair : *map) {
                    std::cout << pair.first << " " << pair.second << std::endl;
                  }
                });
              });
        });
  });
}
