import argparse

import apache_beam as beam
# from apache_beam.io import WriteToText
# from apache_beam.io import ReadFromText

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions


class MyOptions(PipelineOptions):
    """カスタムオプション."""

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input',
            default='./input.txt',
            help='Input path for the pipeline'
        )

        parser.add_argument(
            '--output',
            default='./output.txt',
            help='Output path for the pipeline'
        )


class ComputeWordLength(beam.DoFn):
    """文字数を求める変換処理."""

    def __init__(self):
        pass

    def process(self, element):
        print('element', element)
        yield len(element)
        # return len(element)


def count_check(elem):
    yield len(elem)


def add_hoge(elem):
    yield elem


def run(argv=None):

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        default='./input.txt',
        required=False,
        help='Input path for the pipeline'
    )

    parser.add_argument(
        '--output',
        default='./output.txt',
        required=False,
        help='Output path for the pipeline'
    )

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    # pipeline_options.view_as(SetupOptions).save_main_session = True

    pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'
    # if 'runner' in pipeline_options.get_all_options():
        # gcloud_options = pipeline_options.view_as(GoogleCloudOptions)

    # print('argv', argv)
    # print('parser', parser)
    # print('pipeline_options', pipeline_options)
    # print('gcloud_options', gcloud_options)
    print('pipeline_args', pipeline_args)
    # print('known_args', known_args)
    # print('known_args.input', known_args.input)

    # print('pipeline_options dir', dir(pipeline_options))

    # options = InputOptions()
    # options.view_as(StandardOptions).runner = 'DirectRunner'
    # p = beam.Pipeline(options=options)

    # 1. Pipelineオブジェクトを作成
    # p = beam.Pipeline(argv=pipeline_args)
    # p = beam.Pipeline()

    options = MyOptions()
    options.view_as(StandardOptions).runner = 'DirectRunner'

    p = beam.Pipeline(options=pipeline_options)

    lines = p | u'PCollection作成' >> beam.Create(
        [
            'To be, or not to be: that is the question: ',
            'Whether \'tis nobler in the mind to suffer ',
            'The slings and arrows of outrageous fortune, ',
            'Or to take arms against a sea of troubles, ',
            'Or to take arms against a sea of troubles!!!!!, '
        ]
    )

    lines = (
        lines
        | u"要素の１つ１つにhogeを追加" >> beam.FlatMap(count_check)
        | u"GCS上のストレージに書き込む" >> beam.io.WriteToText(
            known_args.output, num_shards=1
        ))

    # 4. パイプラインを実行する
    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    run()
