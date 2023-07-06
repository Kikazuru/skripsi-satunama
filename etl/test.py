import petl as etl

a = [['foo', 'bar', 'baz'],
     ['A', 1, True]]

b = [['bar', 'foo', 'baz'],
     [2, 'A', False]]

aminusb = etl.recordcomplement(
    etl.cut(a, "foo"), etl.cut(b, "foo"), strict=True)
print(aminusb)
