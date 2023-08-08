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
#include <seastar/core/when_all.hh>
#include <seastar/util/log.hh>

namespace {
seastar::logger logger("word_count");

// The page size for doing I/O operations.
static constexpr size_t kPageSize = 4096;

/**
 * An abstract class that represent the task to be performed on the file. Each
 * task is associated with the FilePrcessor.
 */
class FileProcessorTask {
public:
  virtual ~FileProcessorTask() = default;

  // Called when each character of the file is processed.
  virtual void process(const char &ch) = 0;
};

using FileProcessorTaskPtr = std::unique_ptr<FileProcessorTask>;

/**
 * A class the processes a chunk of the provided file 'filePath'. A chunk is a
 * part of the file and its starting offset is determined by the 'startOffset'.
 * The parameter 'estimatedBytesToRead' is used to determine the estimated size
 * of bytes to read, the processor can read more than the provided chunk size.
 */
class FileProcessor {
public:
  FileProcessor(const std::string &filePath, const size_t &startOffset,
                const size_t &estimatedBytesToRead, FileProcessorTaskPtr &&task)
      : _filePath{filePath}, _fileOffset{startOffset},
        _estimatedBytesToRead{estimatedBytesToRead}, _task{std::move(task)} {}

  seastar::future<FileProcessorTaskPtr> process() {
    return seastar::open_file_dma(_filePath, seastar::open_flags::ro)
        .then([this](auto fileDesc) {
          _fileDesc = std::move(fileDesc);
          return seastar::repeat([this] {
                   return seastar::do_with(
                       // Allocate a buffer aligned to the page size.
                       seastar::allocate_aligned_buffer<char>(kPageSize,
                                                              kPageSize),
                       [this](auto &buffer) {
                         return _fileDesc
                             .dma_read(_fileOffset, buffer.get(), kPageSize)
                             .then([this, &buffer](size_t bytesRead) {
                               return _processBuffer(buffer.get(), bytesRead)
                                          ? seastar::stop_iteration::yes
                                          : seastar::stop_iteration::no;
                             });
                       });
                 })
              .then([this] {
                return seastar::make_ready_future<FileProcessorTaskPtr>(
                    std::move(_task));
              });
        });
  }

private:
  bool _processBuffer(const char *buffer, const size_t &bufferSize) {
    if (bufferSize == 0) {
      return true;
    }

    // Determines if the file processing is complete or not.
    auto isComplete = false;

    // The offset of the buffer where the processing should start.
    auto bufferOffset = _getBufferStartOffset(buffer, bufferSize);

    // If the total bytes read is less than the estimated bytes to read, then
    // continue processing.
    if (_totalBytesRead < _estimatedBytesToRead) {
      std::tie(isComplete, bufferOffset) =
          _processChars(buffer, bufferSize, bufferOffset);
    }

    // Even if the total bytes read is greater than the estimated bytes to read
    // but the chunk is not fully processed because the last character read was
    // not a new line character, then continue processing.
    if (!isComplete && _totalBytesRead >= _estimatedBytesToRead) {
      std::tie(isComplete, bufferOffset) =
          _populateRemaining(buffer, bufferSize, bufferOffset);
    }

    _fileOffset += kPageSize;
    return isComplete;
  }

  size_t _getBufferStartOffset(const char *buffer, const size_t &bufferSize) {
    if (_fileOffset == 0 || _totalBytesRead > 0) {
      return 0;
    }

    // If the chunk start offset lies in the middle of a line, then skip all
    // characters until a new line character is found.
    const char *notFound = buffer + bufferSize;
    const char *result = std::find(buffer, buffer + bufferSize, '\n');
    auto offset = result == notFound ? bufferSize : result - buffer + 1;
    _totalBytesRead += offset;
    return offset;
  }

  std::pair<bool, size_t>
  _processChars(const char *buffer, const size_t &bufferSize, size_t offset) {
    char ch = '\0';
    for (; offset < bufferSize && _totalBytesRead < _estimatedBytesToRead;
         offset++) {
      ch = buffer[offset];
      _invokeTaskProcessor(ch);
    }

    return {ch == '\n', offset};
  }

  std::pair<bool, size_t> _populateRemaining(const char *buffer,
                                             const size_t &bufferSize,
                                             size_t offset) {
    // Process all characters until a new line character is found.
    bool isComplete = false;
    for (; offset < bufferSize; offset++) {
      const char ch = buffer[offset];
      _invokeTaskProcessor(ch);

      if (ch == '\n') {
        isComplete = true;
        break;
      }
    }

    return {isComplete, offset};
  }

  void _invokeTaskProcessor(const char &ch) {
    _totalBytesRead++;
    _task->process(ch);
  }

private:
  const std::string _filePath;
  size_t _fileOffset;
  const size_t _estimatedBytesToRead;
  size_t _totalBytesRead = 0;

  seastar::file _fileDesc;

  FileProcessorTaskPtr _task;
};

using FileProcessorPtr = std::unique_ptr<FileProcessor>;

/**
 * Provides a set of functions that are used by the FileProcessorRunner.
 */
struct FileProcessorRunnerFuncs {
  // Creates a FileProcessorTaskPtr object.
  std::function<FileProcessorTaskPtr()> createTaskFn;

  // Called when all chunks of a file are processed. The parameter contains the
  // processed tasks.
  std::function<void(std::vector<FileProcessorTaskPtr> &&)> onCompleteFn;
};

/**
 * A class that runs file processors. It takes the path to the file to process,
 * splits the file into chunks and allocate each chunk to a file processor. Each
 * file processor runs in a separate shard and process the corresponding chunk
 * of a file. The results of the file processor are collected and passed to the
 * onCompleteFn function.
 */
class FileProcessorRunner {
public:
  FileProcessorRunner(FileProcessorRunnerFuncs &&funcs)
      : _funcs{std::move(funcs)} {
    _app.add_options()("file_path",
                       boost::program_options::value<std::string>()->required(),
                       "Path to the file to process");
  }

  int run(int argc, char **argv) {
    return _app.run(argc, argv, [this] {
      static constexpr const char *kFilePathKey = "file_path";

      auto &&config = _app.configuration();
      const auto filePath = config[kFilePathKey].as<std::string>();

      return seastar::file_stat(filePath).then(
          [this, filePath](seastar::stat_data &&stat) -> seastar::future<> {
            return _startDistributedProcessing(filePath, stat.size);
          });
    });
  }

private:
  /**
   * Provides a vector of promises and corresponding futures that are used to
   * collect the 'FileProcessorTask' from the file processors. The 'onComplete'
   * function calls the 'onResultReadyFn' function when all the results are
   * ready.
   */
  class FileProcessorResults {
  public:
    FileProcessorResults(size_t count) : _promises(count) {
      _futures.reserve(count);
      for (auto &promise : _promises) {
        _futures.push_back(promise.get_future());
      }
    }

    seastar::future<>
    onComplete(std::function<void(std::vector<FileProcessorTaskPtr> &&)>
                   onResultReadyFn) {
      return seastar::when_all_succeed(_futures.begin(), _futures.end())
          .then([onResultReadyFn](auto results) {
            onResultReadyFn(std::move(results));
          });
    }

    void setResult(size_t coreId, FileProcessorTaskPtr &&result) {
      _promises[coreId].set_value(std::move(result));
    }

  private:
    std::vector<seastar::promise<FileProcessorTaskPtr>> _promises;
    std::vector<seastar::future<FileProcessorTaskPtr>> _futures;
  };

  seastar::future<> _startDistributedProcessing(std::string filePath,
                                                size_t fileSize) {
    // Calculate the estimated chunk size aligned to the page size.
    const auto estimatedChunkSize = [&]() {
      size_t chunkSize = fileSize / seastar::smp::count;
      chunkSize = chunkSize / kPageSize * kPageSize;
      return chunkSize > kPageSize ? chunkSize : kPageSize;
    }();

    // An instance of 'FileProcessorResults' is used to collect the result from
    // each file processor and then wait for all results to be ready.
    auto taskResults =
        std::make_shared<FileProcessorResults>(seastar::smp::count);

    return seastar::smp::invoke_on_all(
               [this, filePath, fileSize, estimatedChunkSize, taskResults] {
                 auto coreId = seastar::this_shard_id();
                 auto processor = _createProcessors(coreId, filePath, fileSize,
                                                    estimatedChunkSize);
                 return seastar::do_with(
                     std::move(processor), [taskResults](auto &processor) {
                       return processor->process().then(
                           [taskResults](auto &&processorTask) {
                             // Move the processed task to the 'taskResults' for
                             // a particular shard.
                             taskResults->setResult(seastar::this_shard_id(),
                                                    std::move(processorTask));
                           });
                     });
               })
        .then([this, taskResults] {
          // Wait until all results are ready and then call the provided
          // 'onCompleteFn' function.
          return taskResults->onComplete(_funcs.onCompleteFn);
        });
  }

  FileProcessorPtr _createProcessors(size_t coreId, std::string filePath,
                                     size_t fileSize,
                                     size_t estimatedChunkSize) {
    const size_t offset = coreId * estimatedChunkSize;
    const size_t estimatedBytesToRead =
        coreId != seastar::smp::count - 1
            ? estimatedChunkSize
            : fileSize - (seastar::smp::count - 2) * estimatedChunkSize;

    return std::make_unique<FileProcessor>(filePath, offset,
                                           estimatedBytesToRead,
                                           std::move(_funcs.createTaskFn()));
  }

  seastar::app_template _app;

  std::shared_ptr<std::vector<FileProcessorPtr>> _processors =
      std::make_shared<std::vector<FileProcessorPtr>>();

  FileProcessorRunnerFuncs _funcs;
};

/**
 * A class the computes the count of each word in a file. After the file is
 * fully processed, the results from each shard are aggregated and the total
 * count of each word is printed.
 */
class WordCountTask : public FileProcessorTask {
public:
  void process(const char &ch) final {
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

  static void onComplete(std::vector<FileProcessorTaskPtr> &&tasks) {
    std::unordered_map<std::string, size_t> aggWordCountMap;
    size_t total_words = 0;
    for (auto &&task : tasks) {
      auto &wordCountTask = dynamic_cast<WordCountTask &>(*task);
      for (auto &&[word, count] : *wordCountTask._wordCountMap) {
        aggWordCountMap[word] += count;
        total_words += count;
      }
    }

    // NOTE: This code could stall the shard 0 for some time. The future author
    // could write the result to a file.
    logger.info("--Word count report: Total: {}", total_words);
    for (auto &&[word, count] : aggWordCountMap) {
      logger.info(" {}: {}", word, count);
    }
  }

  static FileProcessorTaskPtr create() {
    return std::make_unique<WordCountTask>();
  }

private:
  std::string _word;
  std::unique_ptr<std::unordered_map<std::string, size_t>> _wordCountMap =
      std::make_unique<std::unordered_map<std::string, size_t>>();
};
} // namespace

int main(int argc, char **argv) {
  seastar::app_template app;
  FileProcessorRunner processor(
      {WordCountTask::create, WordCountTask::onComplete});
  return processor.run(argc, argv);
}
