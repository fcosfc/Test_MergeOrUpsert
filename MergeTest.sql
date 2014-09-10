/* -----------------------------------------------------------
 * MergeTest.sql : test of performance of a MERGE statement
 *                 in front of an UPSERT one.
 *
 * Author: Francisco Saucedo (http://fcosfc.wordpress.com)
 *
 * Versioning:
 *
 *    v1.0, 05-19-2011: Initial version.
 *
 * License: GNU GPL (http://www.gnu.org/licenses/gpl-3.0.html)
 * ----------------------------------------------------------- */
 
SET SERVEROUTPUT ON;

-- Prepare the environment for the UPSERT test
CREATE TABLE merge_tests AS
SELECT object_id,
  owner,
  object_type,
  object_name,
  1 counter
FROM all_objects
WHERE object_type = 'TABLE';
ALTER TABLE merge_tests ADD CONSTRAINT merge_tests_pk PRIMARY KEY (object_id);

EXECUTE runstats_pkg.rs_start;

-- UPSERT test
DECLARE
  TYPE object_types_array IS VARRAY(4) OF VARCHAR2(15);
  object_types object_types_array := object_types_array('TABLE', 'INDEX', 'VIEW', 'TRIGGER');
BEGIN
  FOR i IN 1..4
  LOOP
    FOR j IN 1..i
    LOOP
      FOR reg IN
      (SELECT object_id,
        owner,
        object_type,
        object_name
      FROM all_objects
      WHERE object_type = object_types(j)
      )
      LOOP
        BEGIN
          -- Some complex processes here
          INSERT
          INTO merge_tests
            (
              object_id,
              owner,
              object_type,
              object_name,
              counter
            )
            VALUES
            (
              reg.object_id,
              reg.owner,
              reg.object_type,
              reg.object_name,
              1
            );
        EXCEPTION
        WHEN dup_val_on_index THEN
          UPDATE merge_tests SET counter = counter + 1 WHERE object_id = reg.object_id;
        END;
      END LOOP;
    END LOOP;
  END LOOP;
  COMMIT;
END;
/

EXECUTE runstats_pkg.rs_pause;

-- Prepare the environment for executing the MERGE test with the same conditions
DROP TABLE merge_tests;
CREATE TABLE merge_tests AS
SELECT object_id,
  owner,
  object_type,
  object_name,
  1 counter
FROM all_objects
WHERE object_type = 'TABLE';
ALTER TABLE merge_tests ADD CONSTRAINT merge_tests_pk PRIMARY KEY (object_id);

EXECUTE runstats_pkg.rs_resume;

-- MERGE test
DECLARE
  TYPE object_types_array IS VARRAY(4) OF VARCHAR2(15);
  object_types object_types_array := object_types_array('TABLE', 'INDEX', 'VIEW', 'TRIGGER');
BEGIN
  FOR i IN 1..4
  LOOP
    FOR j IN 1..i
    LOOP
      FOR reg IN
      (SELECT object_id,
        owner,
        object_type,
        object_name
      FROM all_objects
      WHERE object_type = object_types(j)
      )
      LOOP
        -- Some complex processes here
        MERGE INTO merge_tests USING
        (SELECT reg.object_id object_id,
          reg.owner owner,
          reg.object_type object_type,
          reg.object_name object_name
        FROM dual
        ) r ON (merge_tests.object_id = r.object_id)
      WHEN matched THEN
        UPDATE SET merge_tests.counter = merge_tests.counter + 1 WHEN NOT matched THEN
        INSERT
          (
            object_id,
            owner,
            object_type,
            object_name,
            counter
          )
          VALUES
          (
            r.object_id,
            r.owner,
            r.object_type,
            r.object_name,
            1
          );
      END LOOP;
    END LOOP;
  END LOOP;
  COMMIT;
END;
/

EXECUTE runstats_pkg.rs_stop(1000);

-- Clean the environment
DROP TABLE merge_tests;

-- END MergeTest.sql
