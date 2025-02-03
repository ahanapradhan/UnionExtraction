import sys

from openai import OpenAI

from .footballdb_benchmark_queries import Q889_text, Q889_seed, Q259_text, Q51_text

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()

schema = """create table club
(
    club_id    varchar not null
        constraint club_pk
            primary key,
    club_name  varchar not null,
    country    varchar,
    found_year integer
);

create table club_league_history
(
    club_id    varchar
        constraint club_league_history_club_id_fk
            references club,
    league_id  varchar
        constraint club_league_history_league_id_fk
            references league,
    start_year integer,
    end_year   integer
);

create table coach
(
    nickname     varchar,
    coach_id     integer not null
        constraint coach_pk
            primary key,
    country_code varchar,
    wikidata_id  varchar,
    coach_name   varchar,
    similarity   real
);

create table coach_club_team
(
    coach_id          integer
        constraint coach_coach_club_team_fk
            references coach,
    club_id           varchar
        constraint coach_club_team_club_id_fk
            references club,
    club_team_name    varchar,
    start_year        integer not null,
    end_year          integer default 9999,
    coach_wikidata_id varchar
);

create table league
(
    league_id   varchar not null
        constraint league_pk
            primary key,
    league_name varchar,
    found_year  integer,
    country     varchar
);

create table match
(
    match_id                  integer not null
        constraint match_pk
            primary key,
    year                      integer not null
        constraint match_world_cup_fk
            references world_cup,
    round_id                  integer,
    datetime                  timestamp,
    stage                     varchar,
    win_conditions            varchar,
    referee                   varchar,
    assistant_1               varchar,
    assistant_2               varchar,
    attendance                integer,
    stadium_id                integer
        constraint match_stadium_fk
            references stadium,
    did_home_win              boolean,
    is_draw                   boolean,
    home_team_id              integer
        constraint match_national_team_home_team_id_team_id__fk
            references national_team,
    away_team_id              integer
        constraint match_national_team_away_team_id_team_id_fk
            references national_team,
    half_time_home_goals      integer,
    home_team_goals           integer,
    half_time_away_team_goals integer,
    away_team_goals           integer
);

create table national_team
(
    team_id       integer not null
        constraint national_team_pk
            primary key,
    teamname      varchar,
    team_initials varchar(3),
    goals         integer,
    year          integer
);

create table player
(
    nickname    varchar,
    player_id   integer not null
        constraint player_pk
            primary key,
    wikidata_id varchar,
    player_name varchar,
    similarity  real,
    dob         timestamp
);

create table stadium
(
    stadium_id   integer not null
        constraint stadium_pk
            primary key,
    stadium_name varchar,
    city         varchar,
    capacity     integer,
    country      varchar,
    continent    varchar
);

create table world_cup
(
    year           integer not null
        constraint world_cup_pk
            primary key,
    venue          varchar,
    goals_scored   integer,
    qualified_team integer,
    matches_played integer,
    attendance     integer,
    winner         integer
        constraint world_cup_national_team_winner_team_id_fk
            references national_team,
    runner_up      integer
        constraint world_cup_national_team_runner_up_team_id_fk
            references national_team,
    third          integer
        constraint world_cup_national_team_third_team_id_fk
            references national_team,
    fourth         integer
        constraint world_cup_national_team_fourth_team_id_fk
            references national_team
);"""

text_2_sql_prompt = f"You are an expert in formulating SQL from English text." \
                    f"give me SQL query (only the SQL, " \
                    f"without any further explaination) which fits best with the following question: " \
                    f"{Q51_text}. " \
                    f"Use the following schema for your SQL: " \
                    f"{schema}"
def one_round():
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "user",
                "content": f"{text_2_sql_prompt}",
            },
        ], temperature=0, stream=False
    )
    reply = response.choices[0].message.content
    print(reply)


orig_out = sys.stdout
f = open('chatgpt_footballdb_sql.py', 'w')
sys.stdout = f
one_round()
sys.stdout = orig_out
f.close()
