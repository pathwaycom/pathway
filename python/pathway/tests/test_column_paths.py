from pathway.internals import column, dtype as dt
from pathway.internals.column_path import ColumnPath
from pathway.internals.column_properties import ColumnProperties
from pathway.internals.graph_runner.path_evaluator import maybe_flatten_input_storage
from pathway.internals.graph_runner.path_storage import Storage


def assert_storages_have_same_paths(a: Storage, b: Storage) -> None:
    assert set(a.get_columns()) == set(b.get_columns())
    for col in a.get_columns():
        assert a.get_path(col) == b.get_path(col)


def test_removal_of_potentially_big_columns_from_storage_1():
    universe = column.Universe()
    str_column: column.Column = column.MaterializedColumn(
        universe=universe, properties=ColumnProperties(dtype=dt.STR)
    )
    int_column: column.Column = column.MaterializedColumn(
        universe=universe, properties=ColumnProperties(dtype=dt.INT)
    )
    input_storage = Storage.new(
        universe,
        {
            str_column: ColumnPath((0,)),
            int_column: ColumnPath((1,)),
        },
    )
    maybe_flat_storage = maybe_flatten_input_storage(input_storage, [int_column])
    expected_storage = Storage.new(
        universe, {int_column: ColumnPath((0,))}, is_flat=True
    )
    assert_storages_have_same_paths(maybe_flat_storage, expected_storage)


def test_removal_of_potentially_big_columns_from_storage_2():
    universe = column.Universe()
    int_column_1: column.Column = column.MaterializedColumn(
        universe=universe, properties=ColumnProperties(dtype=dt.INT)
    )
    int_column_2: column.Column = column.MaterializedColumn(
        universe=universe, properties=ColumnProperties(dtype=dt.INT)
    )
    int_column_3: column.Column = column.MaterializedColumn(
        universe=universe, properties=ColumnProperties(dtype=dt.INT)
    )
    input_storage = Storage.new(
        universe,
        {
            int_column_1: ColumnPath((0,)),
            int_column_2: ColumnPath((1,)),
            int_column_3: ColumnPath((2,)),
        },
    )
    maybe_flat_storage = maybe_flatten_input_storage(input_storage, [int_column_1])
    expected_storage = Storage.new(
        universe, {int_column_1: ColumnPath((0,))}, is_flat=True
    )
    assert_storages_have_same_paths(maybe_flat_storage, expected_storage)


def test_removal_of_potentially_big_columns_from_storage_3():
    universe = column.Universe()
    int_column_1: column.Column = column.MaterializedColumn(
        universe=universe, properties=ColumnProperties(dtype=dt.INT)
    )
    int_column_2: column.Column = column.MaterializedColumn(
        universe=universe, properties=ColumnProperties(dtype=dt.INT)
    )
    input_storage = Storage.new(
        universe,
        {
            int_column_1: ColumnPath((0,)),
            int_column_2: ColumnPath((1,)),
        },
    )
    maybe_flat_storage = maybe_flatten_input_storage(input_storage, [int_column_1])
    expected_storage = Storage.new(
        universe,
        {
            int_column_1: ColumnPath((0,)),
            int_column_2: ColumnPath((1,)),
        },
    )
    assert_storages_have_same_paths(maybe_flat_storage, expected_storage)


def test_removal_of_potentially_big_columns_from_storage_4():
    universe = column.Universe()
    str_column: column.Column = column.MaterializedColumn(
        universe=universe, properties=ColumnProperties(dtype=dt.STR)
    )
    int_column: column.Column = column.MaterializedColumn(
        universe=universe, properties=ColumnProperties(dtype=dt.INT)
    )
    input_storage = Storage.new(
        universe,
        {
            str_column: ColumnPath((0,)),
            int_column: ColumnPath((1,)),
        },
    ).restrict_to([int_column])
    # make sure also works on restricted version
    # str_column is still in the tuple structure
    maybe_flat_storage = maybe_flatten_input_storage(input_storage, [int_column])
    expected_storage = Storage.new(
        universe, {int_column: ColumnPath((0,))}, is_flat=True
    )
    assert_storages_have_same_paths(maybe_flat_storage, expected_storage)


def test_removal_of_potentially_big_columns_from_storage_5():
    universe = column.Universe()
    str_column_1: column.Column = column.MaterializedColumn(
        universe=universe, properties=ColumnProperties(dtype=dt.STR)
    )
    str_column_2: column.Column = column.MaterializedColumn(
        universe=universe, properties=ColumnProperties(dtype=dt.STR)
    )
    int_column: column.Column = column.MaterializedColumn(
        universe=universe, properties=ColumnProperties(dtype=dt.INT)
    )
    input_storage = Storage.new(
        universe,
        {
            str_column_1: ColumnPath((0,)),
            str_column_2: ColumnPath((1,)),
            int_column: ColumnPath((2,)),
        },
    )
    maybe_flat_storage = maybe_flatten_input_storage(
        input_storage, [int_column, str_column_2]
    )
    expected_storage = Storage.new(
        universe,
        {int_column: ColumnPath((0,)), str_column_2: ColumnPath((1,))},
        is_flat=True,
    )
    assert_storages_have_same_paths(maybe_flat_storage, expected_storage)


def test_removal_of_potentially_big_columns_from_storage_6():
    universe = column.Universe()
    str_column_1: column.Column = column.MaterializedColumn(
        universe=universe, properties=ColumnProperties(dtype=dt.STR)
    )
    str_column_2: column.Column = column.MaterializedColumn(
        universe=universe, properties=ColumnProperties(dtype=dt.STR)
    )
    int_column: column.Column = column.MaterializedColumn(
        universe=universe, properties=ColumnProperties(dtype=dt.INT)
    )
    input_storage = Storage.new(
        universe,
        {
            str_column_1: ColumnPath((0,)),
            str_column_2: ColumnPath((0,)),
            int_column: ColumnPath((2,)),
        },
    )
    # two columns with the same path one can be a reference to the other
    # expect no flatten as all fields are kept
    maybe_flat_storage = maybe_flatten_input_storage(
        input_storage, [int_column, str_column_2]
    )
    expected_storage = input_storage
    assert_storages_have_same_paths(maybe_flat_storage, expected_storage)
