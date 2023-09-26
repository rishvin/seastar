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
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/log.hh>

namespace {
seastar::logger logger("word_count");

// The page size for doing i/o operations.
static constexpr size_t k_page_size = 4096;

/**
 * A class that computes the count of each word in a file. after the file is
 * fully processed, the results from each shard are aggregated and the total
 * count of each word is printed.
 */
class word_count_task {
public:
    using ptr = std::unique_ptr<word_count_task>;

    void process(const char& ch) {
        static constexpr const char* k_delimiters = " \n.,:;";

        if (std::strchr(k_delimiters, ch) == nullptr) {
            _word += ch;
            return;
        }

        if (!_word.empty()) {
            (*_word_count_map)[_word]++;
            _word.clear();
        }
    }

    static ptr on_complete(ptr&& task_left, ptr&& task_right) {
        auto reduced_task = create();
        auto& reduce_map = *reduced_task->_word_count_map;

        for (auto&& task : { std::move(task_left), std::move(task_right) }) {
            auto& wc_task = dynamic_cast<word_count_task&>(*task);
            for (auto&& [word, count] : *wc_task._word_count_map) {
                reduce_map[word] += count;
            }
        }

        return reduced_task;
    }

    void print() {
        // Note: This code could stall the shard 0 for some time. The future author
        // could write the result to a file.
        std::stringstream sstream;
        size_t total_words = 0;

        for (auto&& [word, count] : *_word_count_map) {
            sstream << "  " << word << ": " << count << std::endl;
            total_words += count;
        }
        sstream << "\nTotal words: " << total_words << std::endl;
        logger.info("\nWord count report:\n{}", sstream.str());
    }

    static ptr create() { return std::make_unique<word_count_task>(); }

private:
    std::string _word;
    std::unique_ptr<std::unordered_map<std::string, size_t>> _word_count_map
        = std::make_unique<std::unordered_map<std::string, size_t>>();
};

/**
 * A class that processes a chunk of the provided file file_path. A chunk is a
 * part of the file and its starting offset is determined by the start_offset.
 * The parameter estimated_bytes_to_read is used to determine the estimated
 * size of bytes to read, the processor can read more than the provided chunk
 * size.
 */
class word_count_processor {
public:
    word_count_processor(const std::string& file_path, const size_t& start_offset,
                         const size_t& estimated_bytes_to_read, word_count_task::ptr&& task)
        : _file_path{ file_path },
          _start_offset{ start_offset },
          _estimated_bytes_to_read{ estimated_bytes_to_read },
          _task{ std::move(task) } {}

    seastar::future<word_count_task::ptr> process() {
        return seastar::open_file_dma(_file_path, seastar::open_flags::ro)
            .then([this](auto file_desc) {
                _input_stream = std::move(make_file_input_stream(std::move(file_desc)));
                return _input_stream.skip(_start_offset).then([this] {
                    return seastar::repeat([this] {
                               return _input_stream.read_exactly(k_page_size).then([this](auto&& buffer) {
                                   return _process_buffer(buffer.get(), buffer.size()) ?
                                              seastar::stop_iteration::yes :
                                              seastar::stop_iteration::no;
                               });
                           })
                        .then([this] {
                            return seastar::make_ready_future<word_count_task::ptr>(
                                std::move(_task));
                        });
                });
            });
    }

private:
    bool _process_buffer(const char* buffer, const size_t& buffer_size) {
        if (buffer_size == 0) { return true; }

        // Determines if the file processing is complete or not.
        auto is_complete = false;

        // The offset of the buffer where the processing should start.
        auto buffer_offset = _get_buffer_start_offset(buffer, buffer_size);

        // If the total bytes read is less than the estimated bytes to read, then
        // continue processing.
        if (_total_bytes_read < _estimated_bytes_to_read) {
            std::tie(is_complete, buffer_offset)
                = _process_chars(buffer, buffer_size, buffer_offset);
        }

        // Even if all bytes of the chunk are processed, a chunk is considered not
        // fully processed if the last character read was not a new line character.
        // The processing should continue until a new line character is found.
        if (!is_complete && _total_bytes_read >= _estimated_bytes_to_read) {
            std::tie(is_complete, buffer_offset)
                = _populate_remaining(buffer, buffer_size, buffer_offset);
        }

        return is_complete;
    }

    size_t _get_buffer_start_offset(const char* buffer, const size_t& buffer_size) {
        if (_start_offset == 0 || _total_bytes_read > 0) { return 0; }

        // If the chunk start offset lies in the middle of a line, then skip all
        // characters until a new line character is found.
        const char* not_found = buffer + buffer_size;
        const char* result = std::find(buffer, buffer + buffer_size, '\n');
        auto offset = result == not_found ? buffer_size : result - buffer + 1;
        _total_bytes_read += offset;
        return offset;
    }

    std::pair<bool, size_t> _process_chars(const char* buffer, const size_t& buffer_size,
                                           size_t offset) {
        char ch = '\0';
        for (; offset < buffer_size && _total_bytes_read < _estimated_bytes_to_read; offset++) {
            ch = buffer[offset];
            _invoke_task_processor(ch);
        }

        return { ch == '\n', offset };
    }

    std::pair<bool, size_t> _populate_remaining(const char* buffer, const size_t& buffer_size,
                                                size_t offset) {
        // Process all characters until a new line character is found.
        bool is_complete = false;
        for (; offset < buffer_size; offset++) {
            const char ch = buffer[offset];
            _invoke_task_processor(ch);

            if (ch == '\n') {
                is_complete = true;
                break;
            }
        }

        return { is_complete, offset };
    }

    void _invoke_task_processor(const char& ch) {
        _total_bytes_read++;
        _task->process(ch);
    }

private:
    const std::string _file_path;

    const size_t _start_offset;
    const size_t _estimated_bytes_to_read;

    size_t _total_bytes_read = 0;

    seastar::input_stream<char> _input_stream;

    word_count_task::ptr _task;
};

using word_count_processor_ptr = std::unique_ptr<word_count_processor>;

/**
 * A class that runs file processors. it takes the path to the file to process,
 * splits the file into chunks and allocate each chunk to a file processor. Each
 * file processor runs in a separate shard and process the corresponding chunk
 * of a file. The results of the file processor are collected and passed to the
 * on_complete_fn function.
 */
class word_count_runner {
public:
    word_count_runner() {
        _app.add_options()("file_path", boost::program_options::value<std::string>()->required(),
                           "path to the file to process");
    }

    int run(int argc, char** argv) {
        return _app.run(argc, argv, [this] {
            static constexpr const char* k_file_path_key = "file_path";

            auto&& config = _app.configuration();
            const auto file_path = config[k_file_path_key].as<std::string>();

            return seastar::file_stat(file_path).then(
                [this, file_path](seastar::stat_data&& stat) -> seastar::future<> {
                    return _start_distributed_processing(file_path, stat.size);
                });
        });
    }

private:
    seastar::future<> _start_distributed_processing(std::string file_path, size_t file_size) {
        return _file_metadata.start(file_path, file_size)
            .then([this] {
                return _file_metadata.map_reduce0(
                    [this](auto file_metadata) {
                        auto [file_path, file_size] = file_metadata;
                        auto processor = _create_processor(
                            seastar::this_shard_id(), file_path, file_size);

                        return seastar::do_with(std::move(processor), [](auto& processor) {
                            return processor->process().then([](auto&& task) {
                                return std::move(task);
                            });
                        });
                    },
                    word_count_task::create(),
                    word_count_task::on_complete);
            })
            .then([this](auto&& task) {
                task->print();
                return _file_metadata.stop();
            });
    }

    word_count_processor_ptr _create_processor(
        size_t core_id, std::string file_path, size_t file_size) {
        // Calculate the estimated chunk size aligned to the page size.
        const auto estimated_chunk_size = [&]() {
            size_t chunk_size = file_size / seastar::smp::count;
            chunk_size = chunk_size / k_page_size * k_page_size;
            return chunk_size > k_page_size ? chunk_size : k_page_size;
        }();

        const size_t offset = core_id * estimated_chunk_size;
        const size_t estimated_bytes_to_read
            = core_id != seastar::smp::count - 1 ?
                  estimated_chunk_size :
                  file_size - (seastar::smp::count - 2) * estimated_chunk_size;

        return std::make_unique<word_count_processor>(file_path, offset, estimated_bytes_to_read,
                                                      word_count_task::create());
    }

    seastar::app_template _app;

    seastar::sharded<std::tuple<std::string, size_t>> _file_metadata;
};

} // namespace

int main(int argc, char** argv) {
    seastar::app_template app;
    word_count_runner runner{};
    return runner.run(argc, argv);
}
