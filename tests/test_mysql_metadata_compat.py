from server.common.mysql_metadata_compat import normalize_mysql_referential_rule


def test_no_action_and_restrict_have_one_mysql_semantic_label() -> None:
    assert normalize_mysql_referential_rule("NO ACTION") == "RESTRICT"
    assert normalize_mysql_referential_rule("restrict") == "RESTRICT"


def test_other_referential_actions_remain_distinct() -> None:
    assert normalize_mysql_referential_rule("CASCADE") == "CASCADE"
    assert normalize_mysql_referential_rule("SET NULL") == "SET NULL"
    assert normalize_mysql_referential_rule("") == ""
