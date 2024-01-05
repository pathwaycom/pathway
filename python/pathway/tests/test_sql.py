# Copyright © 2024 Pathway

import pytest

import pathway as pw
from pathway.tests.utils import T, assert_table_equality, assert_table_equality_wo_index


def test_select_1():
    tab = T(
        """
    a | b
    2 | 3
    5 | 6
    """
    )

    assert_table_equality(pw.sql("SELECT a FROM tab", tab=tab), tab.select(tab.a))


def test_select_2():
    tab = T(
        """
    a | b
    2 | 3
    5 | 6
    """
    )
    assert_table_equality(
        pw.sql("SELECT a, b, 1 as c, a+b+1 as d FROM tab", tab=tab),
        tab.select(tab.a, tab.b, c=1, d=tab.a + tab.b + 1),
    )


def test_where():
    tab = T(
        """
    a | b
    1 | 3
    2 | 4
    5 | 2
    """
    )
    assert_table_equality(
        pw.sql("SELECT a, b FROM tab WHERE a>b", tab=tab),
        tab.filter(pw.this.a > pw.this.b),
    )
    assert_table_equality(
        pw.sql("SELECT a, b FROM tab WHERE NOT (a>b)", tab=tab),
        tab.filter(~(pw.this.a > pw.this.b)),
    )


def test_star():
    tab = T(
        """
    a | b
    2 | 3
    5 | 6
    """
    )
    assert_table_equality(
        pw.sql("SELECT * FROM tab", tab=tab),
        tab,
    )


def test_tab_star():
    tab = T(
        """
    a | b
    2 | 3
    5 | 6
    """
    )
    assert_table_equality(
        pw.sql("SELECT tab.* FROM tab", tab=tab),
        tab,
    )


def test_with():
    tab = T(
        """
    a | b
    2 | 3
    5 | 6
    """
    )
    assert_table_equality(
        pw.sql(
            "WITH foo AS (SELECT a+1 AS a, b+1 AS b FROM tab) SELECT a+1 AS a, b+1 AS b FROM foo",
            tab=tab,
        ),
        tab.select(a=tab.a + 2, b=tab.b + 2),
    )


def test_dot():
    tab = T(
        """
    a | b
    2 | 3
    5 | 6
    """
    )
    assert_table_equality(
        pw.sql(
            "SELECT tab.a FROM tab",
            tab=tab,
        ),
        tab.select(tab.a),
    )


def test_groupby():
    tab = T(
        """
    a | b
    x | 5
    x | 6
    y | 7
    y | 8
    """
    )
    assert_table_equality_wo_index(
        pw.sql(
            "SELECT a, SUM(b) as col1, COUNT(*) as col2 FROM tab GROUP BY a",
            tab=tab,
        ),
        T(
            """
        a | col1 | col2
        x | 11   | 2
        y | 15   | 2
        """
        ),
    )


def test_where_groupby():
    tab = T(
        """
    a | b
    x | 5
    x | 6
    y | 7
    y | 8
    z | 9
    z | 10
    """
    )
    assert_table_equality_wo_index(
        pw.sql(
            "SELECT a, SUM(b) as col1, COUNT(*) as col2 FROM tab WHERE b<9 GROUP BY a",
            tab=tab,
        ),
        T(
            """
        a | col1 | col2
        x | 11   | 2
        y | 15   | 2
        """
        ),
    )


def test_having():
    tab = T(
        """
    a | b
    x | 5
    x | 6
    y | 7
    y | 8
    z | 9
    z | 10
    z | 11
    """
    )
    assert_table_equality_wo_index(
        pw.sql(
            "SELECT a, SUM(b) as col1, COUNT(*) as col2 FROM tab HAVING COUNT(*)<3 GROUP BY a",
            tab=tab,
        ),
        T(
            """
        a | col1 | col2
        x | 11   | 2
        y | 15   | 2
        """
        ),
    )


def test_where_having():
    tab = T(
        """
    a | b
    x | 5
    x | 6
    y | 7
    y | 8
    z | 9
    z | 10
    z | 11
    """
    )
    assert_table_equality_wo_index(
        pw.sql(
            "SELECT a, SUM(b) as col1, COUNT(*) as col2 FROM tab WHERE b<11 HAVING COUNT(*)<3 GROUP BY a",
            tab=tab,
        ),
        T(
            """
        a | col1 | col2
        x | 11   | 2
        y | 15   | 2
        z | 19   | 2
        """
        ),
    )


def test_bare_sum():
    tab = T(
        """
    col
    5
    6
    7
    8
    9
    10
    11
    """
    )
    assert_table_equality_wo_index(
        pw.sql(
            "SELECT SUM(col) as sumcol FROM tab",
            tab=tab,
        ),
        T(
            """
            sumcol
            56
        """
        ),
    )


def test_table_alias():
    tab = T(
        """
    a | b
    x | 5
    x | 6
    y | 7
    y | 8
    z | 9
    z | 10
    """
    )
    assert_table_equality(pw.sql("SELECT t1.a FROM tab t1", tab=tab), tab.select(tab.a))


def test_nested():
    tab = T(
        """
    a | b
    x | 5
    x | 6
    y | 7
    y | 8
    z | 9
    z | 10
    """
    )
    assert_table_equality(
        pw.sql("SELECT a FROM (SELECT * FROM tab)", tab=tab), tab.select(tab.a)
    )


def test_nested_with_stars():
    tab = T(
        """
    a | b
    x | 5
    x | 6
    y | 7
    y | 8
    z | 9
    z | 10
    """
    )
    assert_table_equality(pw.sql("SELECT * FROM (SELECT * FROM tab)", tab=tab), tab)


def test_implicit_join():
    tab1 = T(
        """
    a | b
    x | 11
    y | 12
    """
    )
    tab2 = T(
        """
    c | d
    x | 13
    y | 14
        """
    )
    assert_table_equality(
        pw.sql(
            "SELECT * FROM tab1, tab2",
            tab1=tab1,
            tab2=tab2,
        ),
        tab1.join(tab2).select(*pw.left, *pw.right),
    )


def test_implicit_join_where():
    tab1 = T(
        """
    a | b
    x | 11
    y | 12
    """
    )
    tab2 = T(
        """
    c | d
    x | 13
    y | 14
        """
    )
    assert_table_equality_wo_index(
        pw.sql(
            "SELECT * FROM tab1, tab2 WHERE tab1.a=tab2.c",
            tab1=tab1,
            tab2=tab2,
        ),
        tab1.join(tab2, tab1.a == tab2.c).select(*pw.left, *pw.right),
    )


def test_join_where_groupby():
    tab1 = T(
        """
    a | b
    x | 11
    x | 12
    x | 13
    y | 14
    y | 15
    y | 16
    """
    )
    tab2 = T(
        """
    a | c
    x | 11
    x | 12
    x | 13
    y | 14
    y | 15
    y | 16
        """
    )

    expected = T(
        f"""
        col
        {13*11+13*12+12*11}
        """
    )

    result = pw.sql(
        """SELECT SUM(b*c) as col
           FROM tab1 JOIN tab2 ON tab1.a=tab2.a
           GROUP BY a
           WHERE tab1.b > tab2.c
           HAVING tab1.a == 'x'""",
        tab1=tab1,
        tab2=tab2,
    )
    assert_table_equality_wo_index(result, expected)


def test_surprising_selfjoin():
    tab = T(
        """
    a | b
    x | 11
    y | 12
    """
    )
    assert_table_equality(
        pw.sql(
            "SELECT tab1.a AS t1a, tab2.a AS t2a FROM tab1, tab2",
            tab1=tab,
            tab2=tab,
        ),
        tab.join(tab.copy()).select(t1a=pw.left.a, t2a=pw.right.a),
    )


def test_implicit_selfjoin():
    tab = T(
        """
    a | b
    x | 11
    y | 12
    """
    )
    assert_table_equality(
        pw.sql(
            "SELECT t1.a, t2.b FROM tab t1, tab t2",
            tab=tab,
        ),
        tab.join(tab.copy()).select(pw.left.a, pw.right.b),
    )


def test_explicit_join():
    tab1 = T(
        """
    a | b
    x | 11
    y | 12
    """
    )
    tab2 = T(
        """
    c | d
    x | 13
    y | 14
        """
    )
    assert_table_equality(
        pw.sql(
            "SELECT * FROM (tab1 JOIN tab2)",
            tab1=tab1,
            tab2=tab2,
        ),
        tab1.join(tab2).select(*pw.left, *pw.right),
    )
    assert_table_equality(
        pw.sql(
            "SELECT * FROM tab1 JOIN tab2",
            tab1=tab1,
            tab2=tab2,
        ),
        tab1.join(tab2).select(*pw.left, *pw.right),
    )


def test_join_on():
    tab1 = T(
        """
    a | b
    x | 11
    y | 12
    """
    )
    tab2 = T(
        """
    c | d
    x | 13
    y | 14
        """
    )
    assert_table_equality(
        pw.sql(
            "SELECT * FROM tab1 JOIN tab2 ON tab1.a=tab2.c",
            tab1=tab1,
            tab2=tab2,
        ),
        tab1.join(tab2, tab1.a == tab2.c).select(*pw.left, *pw.right),
    )


def test_join_using():
    tab1 = T(
        """
    a | b
    x | 11
    y | 12
    """
    )
    tab2 = T(
        """
    a | d
    x | 13
    y | 14
        """
    )
    assert_table_equality(
        pw.sql(
            "SELECT * FROM tab1 JOIN tab2 USING(a)",
            tab1=tab1,
            tab2=tab2,
        ),
        tab1.join(tab2, tab1.a == tab2.a).select(*pw.this),
    )


def test_union():
    tab1 = T(
        """
    a | b
    x | 11
    y | 12
    z | 13
        """
    )

    tab2 = T(
        """
    a | b
    y | 12
    z | 13
    v | 14
        """
    )

    assert_table_equality_wo_index(
        pw.sql("SELECT * FROM tab1 UNION SELECT * FROM tab2", tab1=tab1, tab2=tab2),
        T(
            """
    a | b
    x | 11
    y | 12
    z | 13
    v | 14
        """
        ),
    )
    assert_table_equality_wo_index(
        pw.sql("SELECT * FROM tab1 UNION ALL SELECT * FROM tab2", tab1=tab1, tab2=tab2),
        T(
            """
    a | b
    x | 11
    y | 12
    z | 13
    y | 12
    z | 13
    v | 14
        """
        ),
    )


# FIXME: change int > float to float > float
# requires adding CAST to pw.sql
def test_advanced_subquery_avg():
    tab = T(
        """
    val | name
      1 |    a
      2 |    b
      3 |    c
      4 |    d
      5 |    e
    """
    )
    assert_table_equality_wo_index(
        pw.sql("SELECT * FROM tab WHERE val > (SELECT AVG(val) FROM tab)", tab=tab),
        T(
            """
    val | name
      4 |    d
      5 |    e
        """
        ),
    )


def test_subquery_having():
    tab = T(
        """
    val | group
      1 |     1
      2 |     1
      3 |     1
      4 |     2
      5 |     2
      6 |     2
    """
    )

    assert_table_equality_wo_index(
        pw.sql(
            "SELECT SUM(val) as sum FROM tab GROUP BY group HAVING group = (SELECT MAX(group) FROM tab)",
            tab=tab,
        ),
        T(
            """
    sum
     15
        """
        ),
    )


@pytest.mark.xfail(reason="Subqueries not supported.")
def test_advanced_subquery_exists():
    first = T(
        """
    val
      1
      2
      3
      4
      5
    """
    )
    second = T(
        """
    val
      3
      4
      5
      6
      7
    """
    )
    assert_table_equality_wo_index(
        pw.sql(
            "SELECT val FROM first WHERE EXISTS(SELECT val FROM second WHERE first.val = second.val)",
            first=first,
            second=second,
        ),
        T(
            """
    val
      3
      4
      5
        """
        ),
    )


def test_case():
    tab = T(
        """
        col
        0
        0
        1
        1
        2
        2
        3
        3
        """
    )
    assert_table_equality(
        pw.sql(
            "SELECT CASE WHEN col=0 THEN 10 WHEN col=1 THEN 11 ELSE 12 END AS col FROM tab",
            tab=tab,
        ),
        T(
            """
        col
        10
        10
        11
        11
        12
        12
        12
        12
        """
        ),
    )


@pytest.mark.xfail(reason="ALL subquery not supported")
def test_advanced_subquery_all():
    first = T(
        """
    val
      1
      2
      3
      4
      5
    """
    )
    second = T(
        """
    val
      3
      4
      5
      6
      7
    """
    )
    assert_table_equality_wo_index(
        pw.sql(
            "SELECT val FROM first WHERE val < ALL(SELECT val FROM second)",
            first=first,
            second=second,
        ),
        T(
            """
    val
      1
      2
        """
        ),
    )


def test_intersect():
    tab1 = T(
        """
    a | b
    x | 11
    x | 11
    y | 12
    z | 13
    z | 13
        """
    )

    tab2 = T(
        """
    a | b
    y | 12
    z | 13
    v | 14
        """
    )

    assert_table_equality_wo_index(
        pw.sql("SELECT * FROM tab1 INTERSECT SELECT * FROM tab2", tab1=tab1, tab2=tab2),
        T(
            """
    a | b
    y | 12
    z | 13
        """
        ),
    )


def test_notimplemented():
    tab = T(
        """
    a | b
    x | 5
    x | 6
    y | 7
    y | 8
    """
    )
    with pytest.raises(NotImplementedError):
        pw.sql("SELECT a, b FROM tab ORDER BY a", tab=tab)

    with pytest.raises(NotImplementedError):
        pw.sql("CREATE TABLE Persons (PersonID int);")


def test_add():
    tab = T(
        """
    a  | b
    10 | 1
    20 | 2
    """
    )
    assert_table_equality_wo_index(
        pw.sql(
            "SELECT a+b as c FROM tab",
            tab=tab,
        ),
        tab.select(c=pw.this.a + pw.this.b),
    )


def test_sub():
    tab = T(
        """
    a  | b
    10 | 1
    20 | 2
    """
    )
    assert_table_equality_wo_index(
        pw.sql(
            "SELECT a-b as c FROM tab",
            tab=tab,
        ),
        tab.select(c=pw.this.a - pw.this.b),
    )


def test_float_div():
    tab = T(
        """
    a  | b
    10 | 1
    20 | 2
    """
    )
    assert_table_equality_wo_index(
        pw.sql(
            "SELECT a/b as c FROM tab",
            tab=tab,
        ),
        tab.select(c=pw.this.a / pw.this.b),
    )


def test_int_div():
    tab = T(
        """
    a  | b
    10 | 1
    20 | 2
    """
    )
    assert_table_equality_wo_index(
        pw.sql(
            "SELECT a DIV b as c FROM tab",
            tab=tab,
        ),
        tab.select(c=pw.this.a // pw.this.b),
    )


def test_mod():
    tab = T(
        """
    a  | b
    10 | 3
    20 | 3
    """
    )
    assert_table_equality_wo_index(
        pw.sql(
            "SELECT a % b as c FROM tab",
            tab=tab,
        ),
        tab.select(c=pw.this.a % pw.this.b),
    )


def test_eq():
    tab = T(
        """
    a  | b
    10 | 3
    20 | 20
    """
    )
    assert_table_equality_wo_index(
        pw.sql(
            "SELECT a == b as c FROM tab",
            tab=tab,
        ),
        tab.select(c=pw.this.a == pw.this.b),
    )


def test_diff():
    tab = T(
        """
    a  | b
    10 | 3
    20 | 20
    """
    )
    assert_table_equality_wo_index(
        pw.sql(
            "SELECT a != b as c FROM tab",
            tab=tab,
        ),
        tab.select(c=pw.this.a != pw.this.b),
    )


def test_geq():
    tab = T(
        """
    a  | b
    10 | 3
    20 | 20
    """
    )
    assert_table_equality_wo_index(
        pw.sql(
            "SELECT a >= b as c FROM tab",
            tab=tab,
        ),
        tab.select(c=pw.this.a >= pw.this.b),
    )


def test_g():
    tab = T(
        """
    a  | b
    10 | 3
    20 | 20
    """
    )
    assert_table_equality_wo_index(
        pw.sql(
            "SELECT a > b as c FROM tab",
            tab=tab,
        ),
        tab.select(c=pw.this.a > pw.this.b),
    )


def test_leq():
    tab = T(
        """
    a  | b
    10 | 3
    20 | 20
    """
    )
    assert_table_equality_wo_index(
        pw.sql(
            "SELECT a <= b as c FROM tab",
            tab=tab,
        ),
        tab.select(c=pw.this.a <= pw.this.b),
    )


def test_l():
    tab = T(
        """
    a  | b
    10 | 3
    20 | 20
    """
    )
    assert_table_equality_wo_index(
        pw.sql(
            "SELECT a < b as c FROM tab",
            tab=tab,
        ),
        tab.select(c=pw.this.a < pw.this.b),
    )


def test_aliases_1():
    tab = T(
        """
        name
        1
        2
        3
        """
    )

    with pytest.raises(KeyError):
        pw.sql("select name from (select name as n from t)", t=tab)


def test_aliases_2():
    tab = T(
        """
        name
        1
        2
        3
        """
    )

    assert_table_equality(
        pw.sql("select n from (select name as n from t) as t", t=tab),
        tab.select(n=tab.name),
    )


def test_aliases_3():
    tab = T(
        """
        name
        1
        2
        3
        """
    )

    assert_table_equality(
        pw.sql("select t1.name as n from (select name as nn from t1) as t2", t1=tab),
        tab.select(n=tab.name),
    )


def test_sql_interview_Q1():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = "SELECT EmpFname AS EmpName FROM EmployeeInfo;"
    assert_table_equality(
        pw.sql(sql_query, EmployeeInfo=EmployeeInfo),
        EmployeeInfo.select(EmpName=pw.this.EmpFname),
    )


@pytest.mark.xfail(reason="UPPER operator not supported")
def test_sql_interview_Q1_upper():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )

    sql_query = (
        "SELECT UPPER(EmpFname) AS EmpName FROM EmployeeInfo;"  # UPPER DOESN'T WORK
    )
    assert_table_equality(
        pw.sql(sql_query, EmployeeInfo=EmployeeInfo),
        EmployeeInfo.select(EmpName=pw.this.EmpFname),
    )


def test_sql_interview_Q2():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = "SELECT COUNT(*) FROM EmployeeInfo WHERE Department = 'HR';"
    res_table = pw.sql(sql_query, EmployeeInfo=EmployeeInfo)
    expected_table = EmployeeInfo.filter(pw.this.Department == "HR").reduce(
        _col_0=pw.reducers.count()
    )
    assert_table_equality(
        res_table,
        expected_table,
    )


@pytest.mark.xfail(reason="KeyError: 'from'")
def test_sql_interview_Q3():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = "SELECT GETDATE();"
    pw.sql(sql_query, EmployeeInfo=EmployeeInfo)


@pytest.mark.xfail(reason="NotImplementedError: SUBSTRING not supported.")
def test_sql_interview_Q4():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = "SELECT SUBSTRING(EmpLname, 1, 4) FROM EmployeeInfo;"
    pw.sql(sql_query, EmployeeInfo=EmployeeInfo)


@pytest.mark.xfail(reason="NotImplementedError: MID not supported.")
def test_sql_interview_Q5():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = "SELECT MID(Address, 0, LOCATE('(',Address)) FROM EmployeeInfo;"
    pw.sql(sql_query, EmployeeInfo=EmployeeInfo)


@pytest.mark.xfail(reason="NotImplementedError: INTO not supported.")
def test_sql_interview_Q6():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = "SELECT * INTO NewTable FROM EmployeeInfo WHERE 1 = 0;"
    pw.sql(sql_query, EmployeeInfo=EmployeeInfo)


@pytest.mark.xfail(reason="NotImplementedError: CREATE TABLE not supported.")
def test_sql_interview_Q6_bis():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = "CREATE TABLE NewTable AS SELECT * FROM EmployeeInfo;"
    pw.sql(sql_query, EmployeeInfo=EmployeeInfo)


def test_sql_interview_Q7():
    EmployeePosition = T(
        """
    EmpID|EmpPosition|DateOfJoining|Salary
    1|Manager|01/05/2022|500000
    2|Executive|02/05/2022|75000
    3|Manager|01/05/2022|90000
    2|Lead|02/05/2022|85000
    1|Executive|01/05/2022|300000
    """
    )
    sql_query = "SELECT * FROM EmployeePosition WHERE Salary BETWEEN 50000 AND 100000;"
    res_table = pw.sql(sql_query, EmployeePosition=EmployeePosition)
    expected_table = EmployeePosition.filter(pw.this.Salary >= 50000)
    expected_table = expected_table.filter(pw.this.Salary <= 100000)
    assert_table_equality(res_table, expected_table)


def test_sql_interview_Q7_bis():
    EmployeePosition = T(
        """
    EmpID|EmpPosition|DateOfJoining|Salary
    1|Manager|01/05/2022|"500000"
    2|Executive|02/05/2022|"75000"
    3|Manager|01/05/2022|"90000"
    2|Lead|02/05/2022|"85000"
    1|Executive|01/05/2022|"300000"
    """
    )
    sql_query = (
        "SELECT * FROM EmployeePosition WHERE Salary BETWEEN '50000' AND '100000';"
    )
    res_table = pw.sql(sql_query, EmployeePosition=EmployeePosition)
    expected_table = EmployeePosition.filter(pw.this.Salary >= "50000")
    expected_table = expected_table.filter(pw.this.Salary <= "100000")
    assert_table_equality(res_table, expected_table)


@pytest.mark.xfail(reason="LIKE 'S%' not supported.")
def test_sql_interview_Q8():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = "SELECT * FROM EmployeeInfo WHERE EmpFname LIKE 'S%';"
    pw.sql(sql_query, EmployeeInfo=EmployeeInfo)


@pytest.mark.xfail(
    reason="sqlglot.errors.ParseError: Required keyword: 'expression' missing"
)
def test_sql_interview_Q9():
    EmployeePosition = T(
        """
    EmpID|EmpPosition|DateOfJoining|Salary
    1|Manager|01/05/2022|500000
    2|Executive|02/05/2022|75000
    3|Manager|01/05/2022|90000
    2|Lead|02/05/2022|85000
    1|Executive|01/05/2022|300000
    """
    )
    sql_query = "SELECT * FROM EmployeePosition ORDER BY Salary DESC LIMIT N;"
    pw.sql(sql_query, EmployeePosition=EmployeePosition)


@pytest.mark.xfail(reason="NotImplementedError: order: ORDER BY not supported.")
def test_sql_interview_Q9_bis():
    EmployeePosition = T(
        """
    EmpID|EmpPosition|DateOfJoining|Salary
    1|Manager|01/05/2022|500000
    2|Executive|02/05/2022|75000
    3|Manager|01/05/2022|90000
    2|Lead|02/05/2022|85000
    1|Executive|01/05/2022|300000
    """
    )
    sql_query = "SELECT * FROM EmployeePosition ORDER BY Salary"
    pw.sql(sql_query, EmployeePosition=EmployeePosition)


@pytest.mark.xfail(
    reason="NotImplementedError: CONCAT(EmployeeInfo.EmpFname, ' ', EmployeeInfo.EmpLname) not supported."
)
def test_sql_interview_Q10():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = (
        "SELECT CONCAT(EmpFname, ' ', EmpLname) AS 'FullName' FROM EmployeeInfo;"
    )
    pw.sql(sql_query, EmployeeInfo=EmployeeInfo)


@pytest.mark.xfail(reason="NotImplementedError: date comparisons")
def test_sql_interview_Q11():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = "SELECT COUNT(*), Gender FROM EmployeeInfo WHERE DOB BETWEEN '02/05/1970 ' AND '31/12/1975' GROUP BY Gender;"  # noqa E501
    res_table = pw.sql(sql_query, EmployeeInfo=EmployeeInfo)
    expected_table = EmployeeInfo.filter(pw.this.DOB >= "02/05/1970")
    expected_table = expected_table.filter(pw.this.DOB <= "31/12/1975")
    expected_table = expected_table.groupby(pw.this.Gender).reduce(
        _col_0=pw.reducers.count(), Gender=pw.this.Gender
    )
    assert_table_equality(res_table, expected_table)


@pytest.mark.xfail(reason="NotImplementedError: ORDER BY")
def test_sql_interview_Q12():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = "SELECT * FROM EmployeeInfo ORDER BY EmpFname desc, Department asc;"
    pw.sql(sql_query, EmployeeInfo=EmployeeInfo)


@pytest.mark.xfail(reason="NotImplementedError: LIKE '____a' not supported.")
def test_sql_interview_Q13():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = "SELECT * FROM EmployeeInfo WHERE EmpLname LIKE '____a';"
    pw.sql(sql_query, EmployeeInfo=EmployeeInfo)


@pytest.mark.xfail(
    reason="NotImplementedError: EmployeeInfo.EmpFname IN ('Sanjay', 'Sonia') not supported."
)
def test_sql_interview_Q14():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = "SELECT * FROM EmployeeInfo WHERE EmpFname NOT IN ('Sanjay','Sonia');"
    pw.sql(sql_query, EmployeeInfo=EmployeeInfo)


@pytest.mark.xfail(
    reason="NotImplementedError: EmployeeInfo.Address LIKE 'DELHI(DEL)%' not supported."
)
def test_sql_interview_Q15():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = "SELECT * FROM EmployeeInfo WHERE Address LIKE 'DELHI(DEL)%';"
    pw.sql(sql_query, EmployeeInfo=EmployeeInfo)


@pytest.mark.xfail(
    reason="NotImplementedError: P.EmpPosition IN ('Manager') not supported in ON clause."
)
def test_sql_interview_Q16():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    EmployeePosition = T(
        """
    EmpID|EmpPosition|DateOfJoining|Salary
    1|Manager|01/05/2022|500000
    2|Executive|02/05/2022|75000
    3|Manager|01/05/2022|90000
    2|Lead|02/05/2022|85000
    1|Executive|01/05/2022|300000
    """
    )
    sql_query = """
    SELECT E.EmpFname, E.EmpLname, P.EmpPosition
    FROM EmployeeInfo E INNER JOIN EmployeePosition P ON
    E.EmpID = P.EmpID AND P.EmpPosition IN ('Manager');
    """
    pw.sql(sql_query, EmployeeInfo=EmployeeInfo, EmployeePosition=EmployeePosition)


def test_sql_interview_Q16_bis():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    EmployeePosition = T(
        """
    EmpID|EmpPosition|DateOfJoining|Salary
    1|Manager|01/05/2022|500000
    2|Executive|02/05/2022|75000
    3|Manager|01/05/2022|90000
    2|Lead|02/05/2022|85000
    1|Executive|01/05/2022|300000
    """
    )
    sql_query = """
    SELECT E.EmpFname, E.EmpLname, P.EmpPosition
    FROM EmployeeInfo E INNER JOIN EmployeePosition P ON
    E.EmpID = P.EmpID AND P.EmpPosition = 'Manager';
    """

    expected_table = (
        EmployeeInfo.join(EmployeePosition, pw.left.EmpID == pw.right.EmpID)
        .filter(pw.right.EmpPosition == "Manager")
        .select(pw.left.EmpFname, pw.left.EmpLname, pw.right.EmpPosition)
    )
    assert_table_equality(
        pw.sql(sql_query, EmployeeInfo=EmployeeInfo, EmployeePosition=EmployeePosition),
        expected_table,
    )


@pytest.mark.xfail(reason="ORDER BY EmpDeptCount not supported.")
def test_sql_interview_Q17():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = """
    SELECT Department, count(EmpID) AS EmpDeptCount
    FROM EmployeeInfo GROUP BY Department
    ORDER BY EmpDeptCount ASC;
    """
    pw.sql(sql_query, EmployeeInfo=EmployeeInfo)


def test_sql_interview_Q17_bis():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = """
    SELECT Department, count(EmpID) AS EmpDeptCount
    FROM EmployeeInfo GROUP BY Department;
    """
    res_table = pw.sql(sql_query, EmployeeInfo=EmployeeInfo)
    expected_table = EmployeeInfo.groupby(pw.this.Department).reduce(
        pw.this.Department, EmpDeptCount=pw.reducers.count()
    )
    assert_table_equality(res_table, expected_table)


@pytest.mark.xfail(reason="NotImplementedError: MOD(rowno, 2) not supported.")
def test_sql_interview_Q18():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = """
    SELECT EmpID FROM (SELECT rowno, EmpID from EmployeeInfo) WHERE MOD(rowno,2)=0;
    """
    pw.sql(sql_query, EmployeeInfo=EmployeeInfo)


@pytest.mark.xfail(reason="TEMPORARY: Missing intermediate value column")
def test_sql_interview_Q19():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    EmployeePosition = T(
        """
    EmpID|EmpPosition|DateOfJoining|Salary
    1|Manager|01/05/2022|500000
    2|Executive|02/05/2022|75000
    3|Manager|01/05/2022|90000
    2|Lead|02/05/2022|85000
    1|Executive|01/05/2022|300000
    """
    )
    sql_query = """
    SELECT * FROM EmployeeInfo E
    WHERE EXISTS
    (SELECT * FROM EmployeePosition P WHERE E.EmpID = P.EmpID);
    """
    res_table = pw.sql(
        sql_query, EmployeeInfo=EmployeeInfo, EmployeePosition=EmployeePosition
    )
    expected_table = EmployeeInfo.join(
        EmployeePosition, pw.left.EmpID == pw.right.EmpID
    ).select(*pw.left)
    expected_table = expected_table.groupby(*pw.this).reduce(*pw.this)
    assert_table_equality_wo_index(res_table, expected_table)


@pytest.mark.xfail(reason="DISTINCT not supported.'")
def test_sql_interview_Q20():
    EmployeePosition = T(
        """
    EmpID|EmpPosition|DateOfJoining|Salary
    1|Manager|01/05/2022|500000
    2|Executive|02/05/2022|75000
    3|Manager|01/05/2022|90000
    2|Lead|02/05/2022|85000
    1|Executive|01/05/2022|300000
    """
    )
    sql_query = """
    SELECT DISTINCT Salary FROM EmployeePosition E1
    WHERE 2 >= (SELECT COUNT(DISTINCT Salary) FROM EmployeePosition E2
    WHERE E1.Salary >= E2.Salary) ORDER BY E1.Salary DESC;
    """
    pw.sql(sql_query, EmployeePosition=EmployeePosition)


@pytest.mark.xfail(reason="Correlated subqueries not supported")
def test_sql_interview_Q21():
    EmployeePosition = T(
        """
    EmpID|EmpPosition|DateOfJoining|Salary
    1|Manager|01/05/2022|500000
    2|Executive|02/05/2022|75000
    3|Manager|01/05/2022|90000
    2|Lead|02/05/2022|85000
    1|Executive|01/05/2022|300000
    """
    )
    sql_query = """
    SELECT Salary
    FROM EmployeePosition E1
    WHERE 3-1 = (
        SELECT COUNT( E2.Salary )
        FROM EmployeePosition E2
        WHERE E2.Salary >  E1.Salary );
    """
    pw.sql(sql_query, EmployeePosition=EmployeePosition)


def test_sql_interview_Q22():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = """
    SELECT EmpID, EmpFname, Department, COUNT(*) as cnt
    FROM EmployeeInfo GROUP BY EmpID, EmpFname, Department
    HAVING COUNT(*) > 1;
    """
    res_table = pw.sql(sql_query, EmployeeInfo=EmployeeInfo)
    expected_table = (
        EmployeeInfo.groupby(pw.this.EmpID, pw.this.EmpFname, pw.this.Department)
        .reduce(
            pw.this.EmpID, pw.this.EmpFname, pw.this.Department, cnt=pw.reducers.count()
        )
        .filter(pw.this.cnt > 1)
    )
    assert_table_equality_wo_index(res_table, expected_table)


def test_sql_interview_Q22_bis():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = """
    SELECT EmpID, EmpFname, Department, COUNT(*) as cnt
    FROM EmployeeInfo GROUP BY EmpID, EmpFname, Department
    HAVING cnt > 1;
    """
    res_table = pw.sql(sql_query, EmployeeInfo=EmployeeInfo)
    expected_table = (
        EmployeeInfo.groupby(pw.this.EmpID, pw.this.EmpFname, pw.this.Department)
        .reduce(
            pw.this.EmpID, pw.this.EmpFname, pw.this.Department, cnt=pw.reducers.count()
        )
        .filter(pw.this.cnt > 1)
    )
    assert_table_equality_wo_index(res_table, expected_table)


@pytest.mark.xfail(reason="NotImplementedError: distinct: DISTINCT not supported.")
def test_sql_interview_Q23():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = """
    Select DISTINCT E.EmpID, E.EmpFname, E.Department
    FROM EmployeeInfo E, EmployeeInfo E1
    WHERE E.Department = E1.Department AND E.EmpID != E1.EmpID;
    """
    pw.sql(sql_query, EmployeeInfo=EmployeeInfo)


def test_sql_interview_Q23_bis():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = """
    Select E.EmpID, E.EmpFname, E.Department
    FROM EmployeeInfo E, EmployeeInfo E1
    WHERE E.Department = E1.Department AND E.EmpID != E1.EmpID;
    """
    res_table = pw.sql(sql_query, EmployeeInfo=EmployeeInfo)
    EmployeeInfoCopy = EmployeeInfo.copy()
    expected_table = (
        EmployeeInfo.join(
            EmployeeInfoCopy,
            pw.left.Department == pw.right.Department,
        )
        .filter(pw.left.EmpID != pw.right.EmpID)
        .select(pw.left.EmpID, pw.left.EmpFname, pw.left.Department)
    )
    assert_table_equality_wo_index(res_table, expected_table)


@pytest.mark.xfail(
    reason="NotImplementedError: order: ORDER BY E.EmpID DESC not supported."
)
def test_sql_interview_Q24():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = """
    SELECT * FROM EmployeeInfo WHERE
    EmpID <=3 UNION SELECT * FROM
    (SELECT * FROM EmployeeInfo E ORDER BY E.EmpID DESC)
    AS E1 WHERE E1.EmpID <=3;
    """
    pw.sql(sql_query, EmployeeInfo=EmployeeInfo)


def test_sql_interview_Q24_bis():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = """
    SELECT * FROM EmployeeInfo WHERE
    EmpID <=3 UNION SELECT * FROM
    (SELECT * FROM EmployeeInfo E)
    AS E1 WHERE E1.EmpID <=3;
    """
    res_table = pw.sql(sql_query, EmployeeInfo=EmployeeInfo)
    expected_table = EmployeeInfo.filter(pw.this.EmpID <= 3)
    assert_table_equality_wo_index(res_table, expected_table)


def test_sql_interview_Q26():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = """
    SELECT * FROM EmployeeInfo WHERE EmpID = (SELECT MIN(EmpID) FROM EmployeeInfo);
    """
    res_table = pw.sql(sql_query, EmployeeInfo=EmployeeInfo)
    expected_table = EmployeeInfo.filter(pw.this.EmpID <= 1)
    assert_table_equality_wo_index(res_table, expected_table)


# TODO, improve Pathway's version
def test_sql_interview_Q26_bis():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = """
    SELECT * FROM EmployeeInfo WHERE EmpID = (SELECT MAX(EmpID) FROM EmployeeInfo);
    """
    res_table = pw.sql(sql_query, EmployeeInfo=EmployeeInfo)
    expected_table = EmployeeInfo.filter(pw.this.EmpID >= 5)
    assert_table_equality_wo_index(res_table, expected_table)


@pytest.mark.xfail(reason="NotImplementedError: REGEXP_LIKE not supported.")
def test_sql_interview_Q27():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = """
    SELECT Email FROM EmployeeInfo WHERE NOT REGEXP_LIKE(Email, '[A-Z0-9._%+-]+@[A-Z0-9.-]+.[A-Z]{2,4}', 'i');
    """
    pw.sql(sql_query, EmployeeInfo=EmployeeInfo)


def test_sql_interview_Q28():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = """
    SELECT Department, COUNT(EmpID) as 'EmpNo' FROM EmployeeInfo GROUP BY Department HAVING COUNT(EmpID) < 2;
    """
    res_table = pw.sql(sql_query, EmployeeInfo=EmployeeInfo)
    expected_table = (
        EmployeeInfo.groupby(pw.this.Department)
        .reduce(pw.this.Department, EmpNo=pw.reducers.count())
        .filter(pw.this.EmpNo < 2)
    )
    assert_table_equality(res_table, expected_table)


def test_sql_interview_Q29():
    EmployeePosition = T(
        """
    EmpID|EmpPosition|DateOfJoining|Salary
    1|Manager|01/05/2022|500000
    2|Executive|02/05/2022|75000
    3|Manager|01/05/2022|90000
    2|Lead|02/05/2022|85000
    1|Executive|01/05/2022|300000
    """
    )
    sql_query = """
    SELECT EmpPosition, SUM(Salary) from EmployeePosition GROUP BY EmpPosition;
    """
    res_table = pw.sql(sql_query, EmployeePosition=EmployeePosition)
    expected_table = EmployeePosition.groupby(pw.this.EmpPosition).reduce(
        pw.this.EmpPosition, _col_1=pw.reducers.sum(pw.this.Salary)
    )
    assert_table_equality(res_table, expected_table)


# TODO, improve the Pathway version
def test_sql_interview_Q30():
    EmployeeInfo = T(
        """
    EmpID|EmpFname|EmpLname|Department|Project|Address|DOB|Gender
    1|Sanjay|Mehra|HR|P1|Hyderabad(HYD)|01/12/1976|M
    2|Ananya|Mishra|Admin|P2|Delhi(DEL)|02/05/1968|F
    3|Rohan|Diwan|Account|P3|Mumbai(BOM)|01/01/1980|M
    4|Sonia|Kulkarni|HR|P1|Hyderabad(HYD)|02/05/1992|F
    5|Ankit|Kapoor|Admin|P2|Delhi(DEL)|03/07/1994|M
    """
    )
    sql_query = """
    SELECT *
    FROM EmployeeInfo WHERE
    EmpID <= (SELECT COUNT(EmpID) DIV 2 from EmployeeInfo);
    """
    res_table = pw.sql(sql_query, EmployeeInfo=EmployeeInfo)
    expected_table = EmployeeInfo.filter(pw.this.EmpID <= 2)
    assert_table_equality_wo_index(res_table, expected_table)
