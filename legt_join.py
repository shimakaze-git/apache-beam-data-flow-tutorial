import apache_beam as beam


class LeftJoinerFn(beam.DoFn):

    def __init__(self):
        super(LeftJoinerFn, self).__init__()

    def process(self, row, **kwargs):

        print('row', row)
        print('row[0]', row[0])

        right_dict = dict(kwargs['right_list'])
        left_key = row[0]

        if left_key in right_dict:
            for each in row[1]:
                yield each + right_dict[left_key]

        else:
            for each in row[1]:
                yield each


class Display(beam.DoFn):
    def process(self, element):
        # LOG.info(str(element))
        print(str(element))
        yield element

# options=pipeline_options
p = beam.Pipeline()

pcoll1 = [('key1', [[('a', 1)],[('b', 2)], [('c', 3)], [('d', 4)],[('e', 5)], [('f', 6)]]), \
        ('key2',[[('a', 12)],[('b', 21)], [('c', 13)]]), \
        ('key3',[[('a', 21)],[('b', 23)], [('c', 31)]])\
        ]
pcoll2 = [('key1', [[('x', 10)]]), ('key2', [[('x', 20)]])]


left_joined = (
    pcoll1 
    | 'groupby' >> beam.GroupByKey()
    | 'LeftJoiner: JoinValues' >> beam.ParDo(LeftJoinerFn(), right_list=pcoll2)
    | 'Display' >> beam.ParDo(Display())
)
p.run()
