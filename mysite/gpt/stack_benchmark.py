sQ9_text = """This query counts the number of unique user accounts on Stack Overflow who 
posted questions tagged with "perl" after January 1, 2014, which received no answers. 
It filters for users with high reputation and who have provided a website URL in their profile. 
The result gives insight into engaged Perl question askers whose questions went unanswered 
despite their active presence.

Must include post_link table in your formulation."""

sQ9_seed = """Select <unknown> as count 
 From account, answer, post_link, question, site, so_user, tag, tag_question 
 Where account.ac_id = so_user.su_account_id
 and post_link.pl_site_id = question.q_site_id
 and question.q_site_id = site.s_site_id
 and site.s_site_id = so_user.su_site_id
 and so_user.su_site_id = tag.t_site_id
 and post_link.pl_post_id_to = question.q_id
 and question.q_owner_user_id = so_user.su_id
 and site.s_site_name = 'stackoverflow'
 and tag.t_name = 'perl'
 and account.ac_website_url LIKE '%_%'
 and question.q_creation_date >= '2014-01-02'
 and tag_question.tq_site_id = tag.t_site_id and
tag_question.tq_tag_id = tag.t_id and
tag_question.tq_question_id = question.q_id and
 and so_user.su_reputation >= 68;  
 
 Must include post_link table in your formulation."""

stack_schema = """Use the following schema:

CREATE TABLE public.account (
    ac_id integer NOT NULL,
    ac_display_name character varying,
    ac_location character varying,
    ac_about_me character varying,
    ac_website_url character varying
);

CREATE TABLE public.answer (
    an_id integer NOT NULL,
    an_site_id integer NOT NULL,
    an_question_id integer,
    an_creation_date date,
    an_deletion_date date,
    an_score integer,
    an_view_count integer,
    an_body character varying,
    an_owner_user_id integer,
    an_last_editor_id integer,
    an_last_edit_date date,
    an_last_activity_date date,
    an_title character varying
);

CREATE TABLE public.badge (
    b_site_id integer NOT NULL,
    b_user_id integer NOT NULL,
    b_name character varying NOT NULL,
    b_date timestamp without time zone NOT NULL
);


CREATE TABLE public.comment (
    c_id integer NOT NULL,
    c_site_id integer NOT NULL,
    c_post_id integer,
    c_user_id integer,
    c_score integer,
    c_body character varying,
    c_date timestamp without time zone
);

CREATE TABLE public.post_link (
    pl_site_id integer NOT NULL,
    pl_post_id_from integer NOT NULL,
    pl_post_id_to integer NOT NULL,
    pl_link_type integer NOT NULL,
    pl_date date
);

CREATE TABLE public.question (
    q_id integer NOT NULL,
    q_site_id integer NOT NULL,
    q_accepted_answer_id integer,
    q_creation_date timestamp without time zone,
    q_deletion_date timestamp without time zone,
    q_score integer,
    q_view_count integer,
    q_body character varying,
    q_owner_user_id integer,
    q_last_editor_id integer,
    q_last_edit_date timestamp without time zone,
    q_last_activity_date timestamp without time zone,
    q_title character varying,
    q_favorite_count integer,
    q_closed_date timestamp without time zone,
    q_tagstring character varying
);


CREATE TABLE public.site (
    s_site_id integer NOT NULL,
    s_site_name character varying
);


CREATE TABLE public.so_user (
    su_id integer NOT NULL,
    su_site_id integer NOT NULL,
    su_reputation integer,
    su_creation_date date,
    su_last_access_date date,
    su_upvotes integer,
    su_downvotes integer,
    su_account_id integer
);



CREATE TABLE public.tag (
    t_id integer NOT NULL,
    t_site_id integer NOT NULL,
    t_name character varying
);


CREATE TABLE public.tag_question (
    tq_question_id integer NOT NULL,
    tq_tag_id integer NOT NULL,
    tq_site_id integer NOT NULL
);

ALTER TABLE ONLY public.account
    ADD CONSTRAINT account_pkey PRIMARY KEY (ac_id);


ALTER TABLE ONLY public.badge
    ADD CONSTRAINT badge_pkey PRIMARY KEY (b_site_id, b_user_id, b_name, b_date);

ALTER TABLE ONLY public.comment
    ADD CONSTRAINT comment_pkey PRIMARY KEY (c_id, c_site_id);

ALTER TABLE ONLY public.post_link
    ADD CONSTRAINT post_link_pkey PRIMARY KEY (pl_site_id, pl_post_id_from, pl_post_id_to, pl_link_type);


ALTER TABLE ONLY public.question
    ADD CONSTRAINT question_pkey PRIMARY KEY (q_id, q_site_id);


ALTER TABLE ONLY public.site
    ADD CONSTRAINT site_pkey PRIMARY KEY (s_site_id);


ALTER TABLE ONLY public.so_user
    ADD CONSTRAINT so_user_pkey PRIMARY KEY (su_id, su_site_id);

ALTER TABLE ONLY public.tag
    ADD CONSTRAINT tag_pkey PRIMARY KEY (t_id, t_site_id);

ALTER TABLE ONLY public.tag_question
    ADD CONSTRAINT tag_question_pkey PRIMARY KEY (tq_site_id, tq_question_id, tq_tag_id);

ALTER TABLE ONLY public.badge
    ADD CONSTRAINT badge_site_id_fkey FOREIGN KEY (b_site_id) REFERENCES public.site(s_site_id);

ALTER TABLE ONLY public.badge
    ADD CONSTRAINT badge_site_id_fkey1 FOREIGN KEY (b_site_id, b_user_id) REFERENCES public.so_user(su_site_id, su_id);

ALTER TABLE ONLY public.comment
    ADD CONSTRAINT comment_site_id_fkey FOREIGN KEY (c_site_id) REFERENCES public.site(s_site_id);

ALTER TABLE ONLY public.post_link
    ADD CONSTRAINT post_link_site_id_fkey FOREIGN KEY (pl_site_id) REFERENCES public.site(s_site_id);


ALTER TABLE ONLY public.post_link
    ADD CONSTRAINT post_link_site_id_fkey1 FOREIGN KEY (pl_site_id, pl_post_id_to) REFERENCES public.question(q_site_id, q_id);

ALTER TABLE ONLY public.post_link
    ADD CONSTRAINT post_link_site_id_fkey2 FOREIGN KEY (pl_site_id, pl_post_id_from) REFERENCES public.question(q_site_id, q_id);


ALTER TABLE ONLY public.question
    ADD CONSTRAINT question_site_id_fkey FOREIGN KEY (q_site_id) REFERENCES public.site(s_site_id);

ALTER TABLE ONLY public.question
    ADD CONSTRAINT question_site_id_fkey1 FOREIGN KEY (q_site_id, q_owner_user_id) REFERENCES public.so_user(su_site_id, su_id);


ALTER TABLE ONLY public.question
    ADD CONSTRAINT question_site_id_fkey2 FOREIGN KEY (q_site_id, q_last_editor_id) REFERENCES public.so_user(su_site_id, su_id);



ALTER TABLE ONLY public.so_user
    ADD CONSTRAINT so_user_account_id_fkey FOREIGN KEY (su_account_id) REFERENCES public.account(ac_id);

ALTER TABLE ONLY public.so_user
    ADD CONSTRAINT so_user_site_id_fkey FOREIGN KEY (su_site_id) REFERENCES public.site(s_site_id);

ALTER TABLE ONLY public.tag_question
    ADD CONSTRAINT tag_question_site_id_fkey FOREIGN KEY (tq_site_id) REFERENCES public.site(s_site_id);

ALTER TABLE ONLY public.tag_question
    ADD CONSTRAINT tag_question_site_id_fkey1 FOREIGN KEY (tq_site_id, tq_tag_id) REFERENCES public.tag(t_site_id, t_id);


ALTER TABLE ONLY public.tag_question
    ADD CONSTRAINT tag_question_site_id_fkey2 FOREIGN KEY (tq_site_id, tq_question_id) REFERENCES public.question(q_site_id, q_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.tag
    ADD CONSTRAINT tag_site_id_fkey FOREIGN KEY (t_site_id) REFERENCES public.site(s_site_id);
    
Must include post_link table in your formulation.
"""