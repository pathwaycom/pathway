import sys

import pathway as pw

input_path = sys.argv[1]
output_path = sys.argv[2]

t = pw.io.plaintext.read(input_path, mode="static")
pw.io.jsonlines.write(t, output_path)
pw.run()
