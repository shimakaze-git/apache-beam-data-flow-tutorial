import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
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


def run():
    options = MyOptions()
    options.view_as(StandardOptions).runner = 'DirectRunner'

    p = beam.Pipeline(options=options)
    print('p', p)
    print('p type', type(p))

    # lines = (p | 'ReadFromInMemory' >> beam.Create(['To be, or not to be: that is the question: ', 'Whether \'tis nobler in the mind to suffer ', 'The slings and arrows of outrageous fortune, ', 'Or to take arms against a sea of troubles, ']))

    lines = p | 'ReadFromInMemory' >> beam.Create(
        [
            'To be, or not to be: that is the question: ',
            'Whether \'tis nobler in the mind to suffer ',
            'The slings and arrows of outrageous fortune, ',
            'Or to take arms against a sea of troubles, '
        ]
    )

    lines = (
        # p
        lines
        # | 'ReadFromText' >> beam.io.ReadFromText(options.input)
        | 'ComputeWordLength' >> beam.ParDo(ComputeWordLength())
        | 'WriteToText' >> beam.io.WriteToText(options.output, num_shards=1))
    # I/O Transformを適用して、オプションで指定したパスにデータを書き込む

    print('lines', type(lines))

    p.run()


if __name__ == "__main__":
    run()
