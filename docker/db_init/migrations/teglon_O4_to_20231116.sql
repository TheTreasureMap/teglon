-- Bring a database built from the old teglon_O4.sql schema up to teglon_schema_20231116.sql,
-- which the 2.x code needs (ObservedTile.Source and the light-curve fit columns).
-- Those two schemas differ only in ObservedTile. Run once; existing rows are kept, and
-- FileName values carry over into Source.
USE teglon;

ALTER TABLE ObservedTile
    CHANGE COLUMN FileName Source varchar(256) DEFAULT NULL,
    MODIFY COLUMN PositionAngle double DEFAULT 0,
    ADD COLUMN x0 double DEFAULT NULL AFTER PositionAngle,
    ADD COLUMN x0_err double DEFAULT NULL AFTER x0,
    ADD COLUMN a double DEFAULT NULL AFTER x0_err,
    ADD COLUMN a_err double DEFAULT NULL AFTER a,
    ADD COLUMN n double DEFAULT 1 AFTER a_err,
    ADD COLUMN n_err double DEFAULT NULL AFTER n;
