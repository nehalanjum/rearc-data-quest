--Access control: bonus/access_controls.sql grants a read-only analyst group access to the Gold tables only. 
--Bronze, Silver and the raw Volume stay restricted. It is written as a script because Free Edition does not allow custom groups, so I have not executed it here.
-- Read-only analyst access to the Gold layer only.
-- Bronze, Silver and the raw Volume stay restricted to the pipeline owner.
-- Replace `data_analysts` with a real group (Account console -> User management -> Groups).


GRANT USE CATALOG ON CATALOG workspace TO `data_analysts`;
GRANT USE SCHEMA  ON SCHEMA  workspace.rearc TO `data_analysts`;

GRANT SELECT ON TABLE workspace.rearc.gold_q1_population_stats           TO `data_analysts`;
GRANT SELECT ON TABLE workspace.rearc.gold_q2_best_year_per_series       TO `data_analysts`;
GRANT SELECT ON TABLE workspace.rearc.gold_q3_series_value_with_population TO `data_analysts`;

-- Verify what the group can see
SHOW GRANTS ON TABLE workspace.rearc.gold_q2_best_year_per_series;
