import apache_beam as beam


class DoFnMethods(beam.DoFn):

    # _list = []

    def __init__(self):
        print("__init__")
        self.window = beam.window.GlobalWindow()
        self._list = []

    def setup(self):
        # print("setup")
        pass

    def start_bundle(self):
        # print("start_bundle")
        pass

    def process(self, element, window=beam.DoFn.WindowParam):
        # print("window", window)
        self.window = window
        self._list.append(element)
        # yield "* process: " + element

    def finish_bundle(self):
        yield beam.utils.windowed_value.WindowedValue(
            value="* finish_bundle: 🌱🌳🌍", timestamp=0, windows=[self.window]
        )

    def teardown(self):
        # print("teardown")
        # print('self._list = []', self._list)
        return self._list
        # yield self._list


with beam.Pipeline() as pipeline:
    results = (
        pipeline
        | "Create inputs" >> beam.Create(["🍓", "🥕", "🍆", "🍅", "🥔"])
        | "DoFn methods" >> beam.ParDo(DoFnMethods())
        # | beam.Map(print)
        | "WriteToText" >> beam.io.WriteToText("./output.txt", num_shards=1)
    )
