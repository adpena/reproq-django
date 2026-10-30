import pytest
import json
from pathlib import Path

from reproq_django.pg_cron import PgCronError, job_name, native_schedule, occurrence_command


def test_names_do_not_collide_after_sanitization_or_truncation():
    names = ['a-b', 'a.b', 'a_b', 'x' * 150 + 'a', 'x' * 150 + 'b', 'é', '']
    labels = [job_name('reproq_periodic', name) for name in names]
    assert len(set(labels)) == len(names)
    assert all(len(label) <= 63 for label in labels)


@pytest.mark.parametrize('name', ["simple", "a'b", "a\\b", "x'); SELECT 1; --", 'été\nnext'])
def test_stored_task_name_is_a_postgresql_literal(name):
    from psycopg import sql

    command = occurrence_command(name)
    literal = sql.Literal(name).as_string()
    assert command == (
        'SELECT * FROM public.reproq_enqueue_periodic_occurrence('
        + literal + ", date_trunc('minute', statement_timestamp()), 'pg_cron');"
    )


def test_native_zone_must_match_the_definition_across_dst():
    assert native_schedule('CRON_TZ=America/Chicago 0 0 * * *', 'America/Chicago') == '0 0 * * *'
    with pytest.raises(PgCronError, match='differs'):
        native_schedule('CRON_TZ=America/Chicago 0 0 * * *', 'GMT')
    with pytest.raises(PgCronError, match='differs'):
        native_schedule('0 0 * * *', 'America/Chicago')
    assert native_schedule('0 0 * * *', 'GMT') == '0 0 * * *'


@pytest.mark.parametrize('expression', ['10 seconds', '@daily', '* * * * * *', '61 * * * *'])
def test_only_portable_five_field_schedules_are_accepted(expression):
    with pytest.raises(ValueError):
        native_schedule(expression, 'UTC')


def test_shared_portable_cron_grammar_vectors():
    from reproq_django.recurring import parse_schedule

    vectors = json.loads((Path(__file__).parents[1] / 'contracts/cron_schedules.json').read_text())
    for expression in vectors['accepted']:
        parse_schedule(expression)
    for expression in vectors['rejected']:
        with pytest.raises(ValueError):
            parse_schedule(expression)
