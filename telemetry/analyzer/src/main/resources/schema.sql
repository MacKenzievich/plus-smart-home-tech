create table if not exists scenarios
(
    id     BIGINT generated always as identity primary key,
    hub_id VARCHAR,
    name   VARCHAR,
    unique (hub_id, name)
);


create table if not exists sensors
(
    id     VARCHAR primary key,
    hub_id VARCHAR
);


create table if not exists conditions
(
    id        BIGINT generated always as identity primary key,
    type      VARCHAR,
    operation VARCHAR,
    value     INTEGER
);


create table if not exists actions
(
    id    BIGINT generated always as identity primary key,
    type  VARCHAR,
    value INTEGER
);


create table if not exists scenario_conditions
(
    scenario_id  BIGINT references scenarios (id),
    sensor_id    VARCHAR references sensors (id),
    condition_id BIGINT references conditions (id),
    primary key (scenario_id, sensor_id, condition_id)
);


create table if not exists scenario_actions
(
    scenario_id BIGINT references scenarios (id),
    sensor_id   VARCHAR references sensors (id),
    action_id   BIGINT references actions (id),
    primary key (scenario_id, sensor_id, action_id)
);


create or replace function check_hub_id() returns TRIGGER as
'
    begin
        if ( select hub_id from scenarios where id = NEW.scenario_id ) !=
           ( select hub_id from sensors where id = NEW.sensor_id ) then
            raise exception ''Hub IDs do not match for scenario_id % and sensor_id %'', NEW.scenario_id, NEW.sensor_id;
        end if;
        return NEW;
    end;
' language plpgsql;


create or replace trigger tr_bi_scenario_conditions_hub_id_check
    before insert
    on scenario_conditions
    for each row
execute function check_hub_id();


create or replace trigger tr_bi_scenario_actions_hub_id_check
    before insert
    on scenario_actions
    for each row
execute function check_hub_id();
