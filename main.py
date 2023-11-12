import argparse
from urllib.parse import unquote, quote
import datetime
import pathlib
import subprocess
import io
import functools

import multiprocessing

import heapq3 as heapq
import itertools

def iter_except(func, exception, first=None):
    """ Call a function repeatedly until an exception is raised.

    Converts a call-until-exception interface to an iterator interface.
    Like builtins.iter(func, sentinel) but uses an exception instead
    of a sentinel to end the loop.

    Examples:
        iter_except(functools.partial(heappop, h), IndexError)   # priority pipe iterator
        iter_except(d.popitem, KeyError)                         # non-blocking dict iterator
        iter_except(d.popleft, IndexError)                       # non-blocking deque iterator
        iter_except(q.get_nowait, Queue.Empty)                   # loop over a producer Queue
        iter_except(s.pop, KeyError)                             # non-blocking set iterator

    """
    try:
        if first is not None:
            yield first()            # For database APIs needing an initial cast to db.first()
        while True:
            yield func()
    except exception:
        pass

def chunked(iterable, n):
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)

def rdd_parse_worker(rdd_path, parser, pipe):
    for line in read_rdd(rdd_path):
        pipe.send(parser(line))
    pipe.send(StopIteration)
    print('parsed rdd:', rdd_path,'. process exiting...')

def parse_rdd_in_process(rdd_path, parser):
    pipe = multiprocessing.Pipe(duplex=False)

    p = multiprocessing.Process(
        target=rdd_parse_worker,
        args=(rdd_path, parser, pipe[1]),
        daemon=True,
    )
    p.start()

    return iter(pipe[0].recv, StopIteration)

def gzip_read(file_path):
    f = subprocess.Popen(['gzip', '-cd', file_path], stdout=subprocess.PIPE)
    return io.TextIOWrapper(f.stdout, encoding='utf-8')

def gzip_write(file_path):
    out = open(file_path, 'wb')
    f = subprocess.Popen('gzip', stdout=out, stdin=subprocess.PIPE)
    return io.TextIOWrapper(f.stdin, encoding='utf-8')

unquote_cached = functools.lru_cache(maxsize=100000)(unquote)
quote_cached = functools.lru_cache(maxsize=100000)(quote)


DATETIME_PATTERN = '%Y%m%d-%H%M%S'
def line_parser_cached(line):
    project, page, timestamp, counts, bytes_trans = line[:-1].split(' ')

    page = unquote_cached(page)
    # timestamp = datetime.datetime.strptime(timestamp, DATETIME_PATTERN)
    counts = int(counts)
    bytes_trans = int(bytes_trans)

    return project, page, timestamp, counts, bytes_trans

def line_parser(line):
    project, page, timestamp, counts, bytes_trans = line[:-1].split(' ')

    page = unquote(page)
    # timestamp = datetime.datetime.strptime(timestamp, DATETIME_PATTERN)
    counts = int(counts)
    bytes_trans = int(bytes_trans)

    return project, page, timestamp, counts, bytes_trans

def line_parser_q(line):
    project, page, timestamp, counts, bytes_trans = line[:-1].split(' ')

    # timestamp = datetime.datetime.strptime(timestamp, DATETIME_PATTERN)
    counts = int(counts)
    bytes_trans = int(bytes_trans)

    return project, page, timestamp, counts, bytes_trans

def read_rdd(rdd_path):
    for part in sorted(rdd_path.glob('part-*.gz')):
        with gzip_read(str(part)) as input_f:
            yield from input_f

def record_to_text(record):
    project, page, timestamp, counts, bytes_trans = record

    return f'{project} {quote(page)} {timestamp} {counts} {bytes_trans}\n'

def record_to_text_cached(record):
    project, page, timestamp, counts, bytes_trans = record

    return f'{project} {quote_cached(page)} {timestamp} {counts} {bytes_trans}\n'

def record_to_text_q(record):
    project, page, timestamp, counts, bytes_trans = record

    return f'{project} {page} {timestamp} {counts} {bytes_trans}\n'

def keyby_project_page(record):
    return record[:2]

def keyby_timestamp(record):
    return record[2]

def reduce_timestamps(records):
    """reduces records with the timestamp, assuming they are all from the same
    (project, page) and ordered.
    """
    def sum_records(record_a, record_b):
        proj, page, timestamp, count_a, bytes_a = record_a
        _, _, _, count_b, bytes_b = record_b
        return (proj, page, timestamp, count_a + count_b, bytes_a + bytes_b)

    for _, group in itertools.groupby(records, key=keyby_timestamp):
        yield functools.reduce(sum_records, group)

def write_from_pipe(file_path, pipe):
    records = iter(pipe.recv, StopIteration)
    with gzip_write(file_path) as output:
        for record in records:
            output.write(record_to_text(record))
    print('writer ended')


def write_to_rdd(output_path, records, formatter, records_per_partition):
    output_path.mkdir(parents=True)

    for index, chunk in enumerate(chunked(records, records_per_partition)):
        file_name = 'part-{index}.gz'.format(index=str(index).zfill(10))
        file_path = output_path/file_name
        output_file = gzip_write(str(output_path/file_name))

        with output_file:
            for record in chunk:
                output_file.write(formatter(record))

        print(file_name, 'written.', flush=True)

        print('stats for nerds: quote:', quote_cached.cache_info())
        print('stats for nerds: unquote:', unquote_cached.cache_info())

def main():
    parser = argparse.ArgumentParser(
        description='Sort already sorted multifolder pagecounts.'
    )
    parser.add_argument(
        'input_folders',
        metavar='INPUT_FOLDER',
        nargs='+',
        type=pathlib.Path,
        help='''Input folder containing tsv data already sorted.''',
    )
    parser.add_argument(
        'output_folder',
        metavar='OUTPUT_FOLDER',
        type=pathlib.Path,
        help="Output folder (it will be xz'd)",
    )
    parser.add_argument(
        '--records-per-partition',
        required=False,
        default=1000000,
        type=int,
        help='Number of output partitions',
    )
    # TODO:
    # parser.add_argument(
    #     '--project', '-p',
    #     required=False,
    #     help='Restrict the merge to only the specified project. E.g. "en", "it", ...',
    # )

    args = parser.parse_args()
    print(args)

    rdds = [
        # parse_rdd_in_process(path, line_parser_cached)
        (line_parser_cached(line) for line in read_rdd(path))
        for path in args.input_folders
    ]

    merged_records = heapq.merge(rdds, key=keyby_project_page)

    records_sorted_and_reduced = (record
        for _, group in itertools.groupby(merged_records, key=keyby_project_page)
        for record in reduce_timestamps(sorted(group, key=keyby_timestamp))
    )

    write_to_rdd(
        args.output_folder,
        records_sorted_and_reduced,
        formatter=record_to_text_cached,
        records_per_partition=args.records_per_partition,
    )

if __name__ == '__main__':
    main()
