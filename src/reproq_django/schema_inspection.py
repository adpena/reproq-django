"""Immutable v1 read-only PostgreSQL catalog inspection shared by consumers.

These helpers bind unqualified identifiers to permanent public relations and
return schema metadata only. They never create, adopt, migrate or repair tables,
read application rows, or choose accepted historical profiles. Future contract
formats must use a new versioned API rather than changing v1 output.
"""

import hashlib
import json


class SchemaInspectionError(RuntimeError):
    def __init__(self, message, *, code="input", relation=None):
        super().__init__(message)
        self.code = code
        self.relation = relation


def _relation_names(values, *, allow_empty=False):
    if isinstance(values, (str, bytes)):
        raise SchemaInspectionError("Relation names must be an iterable of unqualified identifiers.")
    try:
        names = tuple(values)
    except TypeError as exc:
        raise SchemaInspectionError("Relation names must be iterable.") from exc
    if (not names and not allow_empty) or any(not isinstance(n, str) or not n or "\x00" in n for n in names):
        raise SchemaInspectionError("Relation identifiers must be nonempty strings without NUL characters.")
    return tuple(dict.fromkeys(names))


def catalog_sha256_v1(catalog):
    """Preserve the original v1 ASCII JSON fingerprint encoding exactly."""
    return hashlib.sha256(json.dumps(catalog, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def require_public_relations_v1(connection, *, relations, allow_missing=False):
    """Bind unqualified queue SQL to the relations actually certified by v1."""
    if connection.vendor != "postgresql":
        raise SchemaInspectionError("PostgreSQL relation binding is required.", code="vendor")
    relation_names = _relation_names(relations)
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_catalog.current_schema()")
        if cursor.fetchone()[0] != "public":
            raise SchemaInspectionError("Only the public schema is qualified for inspection v1.", code="namespace")
        cursor.execute(
            "SELECT requested.name,n.nspname,c.relkind,c.relpersistence,c.relispartition "
            "FROM pg_catalog.unnest(%s::text[]) AS requested(name) "
            "LEFT JOIN pg_catalog.pg_class c ON c.oid=pg_catalog.to_regclass(pg_catalog.quote_ident(requested.name)) "
            "LEFT JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace",
            [list(relation_names)],
        )
        for name, namespace, kind, persistence, partition in cursor.fetchall():
            if namespace is None and allow_missing:
                continue
            if namespace != "public" or kind != "r" or persistence != "p" or partition:
                raise SchemaInspectionError(
                    f"Relation {name!r} must resolve to an ordinary permanent public table; "
                    "shadow, temporary, view and missing relations cannot certify this schema.",
                    code="relation", relation=name,
                )


def read_public_catalog_v1(connection, *, tables, extra_relations=()):
    if connection.vendor != "postgresql":
        raise SchemaInspectionError("Schema inspection supports PostgreSQL only.", code="vendor")
    table_names = _relation_names(tables)
    extra_names = _relation_names(extra_relations, allow_empty=True)
    require_public_relations_v1(connection, relations=(*table_names, *extra_names), allow_missing=True)
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT c.relname,c.relkind,c.relrowsecurity,c.relforcerowsecurity,"
            "c.relhasrules,c.relispartition,c.relpersistence,c.relreplident,a.amname "
            "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
            "LEFT JOIN pg_catalog.pg_am a ON a.oid=c.relam "
            "WHERE n.nspname='public' AND c.relname=ANY(%s) ORDER BY 1", [list(table_names)]
        )
        tables = cursor.fetchall()
        cursor.execute(
            "SELECT table_name,column_name,udt_name,is_nullable,character_maximum_length,"
            "column_default,is_identity,identity_generation,is_generated,generation_expression,"
            "udt_schema,data_type,domain_schema,domain_name,collation_schema,collation_name,"
            "numeric_precision,numeric_scale,datetime_precision "
            "FROM information_schema.columns WHERE table_schema='public' AND table_name=ANY(%s) "
            "ORDER BY table_name,column_name", [list(table_names)]
        )
        columns = cursor.fetchall()
        cursor.execute(
            "SELECT t.relname,k.conname,k.contype,pg_catalog.pg_get_constraintdef(k.oid,true),"
            "k.convalidated,k.condeferrable,k.condeferred FROM pg_catalog.pg_constraint k "
            "JOIN pg_catalog.pg_class t ON t.oid=k.conrelid JOIN pg_catalog.pg_namespace n ON n.oid=t.relnamespace "
            "WHERE n.nspname='public' AND t.relname=ANY(%s) ORDER BY 1,2", [list(table_names)]
        )
        constraints = cursor.fetchall()
        cursor.execute(
            "SELECT t.relname,i.relname,pg_catalog.pg_get_indexdef(i.oid),x.indisvalid,x.indisready,x.indisunique "
            "FROM pg_catalog.pg_index x JOIN pg_catalog.pg_class t ON t.oid=x.indrelid JOIN pg_catalog.pg_class i ON i.oid=x.indexrelid "
            "JOIN pg_catalog.pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname='public' "
            "AND t.relname=ANY(%s) ORDER BY 1,2", [list(table_names)]
        )
        indexes = cursor.fetchall()
        cursor.execute(
            "SELECT t.relname,g.tgname,g.tgenabled,pg_catalog.pg_get_triggerdef(g.oid),p.proname,"
            "pg_catalog.pg_get_function_identity_arguments(p.oid),pg_catalog.pg_get_function_result(p.oid),"
            "l.lanname,p.prosecdef,p.proconfig,p.prosrc FROM pg_catalog.pg_trigger g "
            "JOIN pg_catalog.pg_class t ON t.oid=g.tgrelid JOIN pg_catalog.pg_namespace n ON n.oid=t.relnamespace "
            "JOIN pg_catalog.pg_proc p ON p.oid=g.tgfoid JOIN pg_catalog.pg_language l ON l.oid=p.prolang "
            "WHERE n.nspname='public' AND t.relname=ANY(%s) AND NOT g.tgisinternal "
            "ORDER BY 1,2", [list(table_names)]
        )
        triggers = []
        for row in cursor.fetchall():
            # Function bodies can contain arbitrary deployment text. Never put
            # their contents in inspection output or audit receipts.
            triggers.append([*row[:-1], hashlib.sha256(row[-1].encode()).hexdigest()])
        cursor.execute(
            "SELECT t.relname,a.attname,s.relname,d.deptype,"
            "q.seqtypid::regtype::text,q.seqstart,q.seqmin,q.seqmax,q.seqincrement,q.seqcycle,q.seqcache "
            "FROM pg_catalog.pg_class s JOIN pg_catalog.pg_namespace n ON n.oid=s.relnamespace "
            "JOIN pg_catalog.pg_depend d ON d.classid='pg_catalog.pg_class'::pg_catalog.regclass AND d.objid=s.oid "
            "AND d.refclassid='pg_catalog.pg_class'::pg_catalog.regclass JOIN pg_catalog.pg_class t ON t.oid=d.refobjid "
            "JOIN pg_catalog.pg_attribute a ON a.attrelid=t.oid AND a.attnum=d.refobjsubid "
            "JOIN pg_catalog.pg_sequence q ON q.seqrelid=s.oid WHERE n.nspname='public' AND s.relkind='S' "
            "AND t.relname=ANY(%s) AND d.deptype IN ('a','i') ORDER BY 1,2,3", [list(table_names)]
        )
        sequences = cursor.fetchall()
        cursor.execute(
            "SELECT c.relname,pn.nspname,p.relname FROM pg_catalog.pg_inherits i "
            "JOIN pg_catalog.pg_class c ON c.oid=i.inhrelid JOIN pg_catalog.pg_namespace cn ON cn.oid=c.relnamespace "
            "JOIN pg_catalog.pg_class p ON p.oid=i.inhparent JOIN pg_catalog.pg_namespace pn ON pn.oid=p.relnamespace "
            "WHERE (cn.nspname='public' AND c.relname=ANY(%s)) "
            "OR (pn.nspname='public' AND p.relname=ANY(%s)) ORDER BY 1,2,3",
            [list(table_names), list(table_names)],
        )
        inheritance = cursor.fetchall()
        cursor.execute(
            "SELECT c.relname,p.polname,p.polcmd,p.polpermissive,"
            "pg_catalog.pg_get_expr(p.polqual,p.polrelid),pg_catalog.pg_get_expr(p.polwithcheck,p.polrelid) "
            "FROM pg_catalog.pg_policy p JOIN pg_catalog.pg_class c ON c.oid=p.polrelid "
            "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='public' AND c.relname=ANY(%s) ORDER BY 1,2", [list(table_names)],
        )
        policies = cursor.fetchall()
    # Make both psycopg rows and checked-in JSON profiles use JSON primitives.
    return json.loads(json.dumps({"tables": tables, "columns": columns, "constraints": constraints,
                                  "indexes": indexes, "triggers": triggers, "sequences": sequences,
                                  "inheritance": inheritance, "policies": policies}))
