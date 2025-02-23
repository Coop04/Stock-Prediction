#!/usr/bin/env python
# -*- coding: utf-8 -*-

# pylint: disable-all

# import from_api as fromApi
import db as db
import time, datetime
import pandas as pd


def DeleteNoneEntryColumnns(tableName):
    q = f"""
    CREATE OR REPLACE FUNCTION delete_null_rows(table_name TEXT) RETURNS VOID AS $$
    DECLARE
        col_name TEXT;
        delete_query TEXT := 'DELETE FROM ' || table_name || ' WHERE ';
        first_col BOOLEAN := TRUE;
    BEGIN
        FOR col_name IN
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = table_name AND table_schema = 'public'
        LOOP
            IF NOT first_col THEN
                delete_query := delete_query || ' OR ';
            END IF;
            delete_query := delete_query || col_name || ' IS NULL';
            first_col := FALSE;
        END LOOP;
        delete_query := delete_query || ';';
        EXECUTE delete_query;
    END;
    $$ LANGUAGE plpgsql;
    """
    res = db.query_database(q, fetch=False)
    print(res)

    q = f"""
        SELECT delete_null_rows('{tableName}');
        """

    res = db.query_database(q, fetch=False)
    print(res)
