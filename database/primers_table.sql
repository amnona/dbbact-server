--
-- PostgreSQL database dump
--

-- Dumped from database version 15.1
-- Dumped by pg_dump version 15.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: primerstable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.primerstable (
    id integer NOT NULL,
    iduser integer NOT NULL,
    forwardprimer text,
    reverseprimer text,
    regionname text,
    fprimerseq text
);


ALTER TABLE public.primerstable OWNER TO dbbact;

--
-- Name: primerstable_id_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.primerstable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.primerstable_id_seq OWNER TO dbbact;

--
-- Name: primerstable_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.primerstable_id_seq OWNED BY public.primerstable.id;


--
-- Name: primerstable_iduser_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.primerstable_iduser_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.primerstable_iduser_seq OWNER TO dbbact;

--
-- Name: primerstable_iduser_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.primerstable_iduser_seq OWNED BY public.primerstable.iduser;


--
-- Name: primerstable id; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.primerstable ALTER COLUMN id SET DEFAULT nextval('public.primerstable_id_seq'::regclass);


--
-- Name: primerstable iduser; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.primerstable ALTER COLUMN iduser SET DEFAULT nextval('public.primerstable_iduser_seq'::regclass);


--
-- Data for Name: primerstable; Type: TABLE DATA; Schema: public; Owner: dbbact
--

COPY public.primerstable (id, iduser, forwardprimer, reverseprimer, regionname, fprimerseq) FROM stdin;
0	0	na	na	na	na
1	0	515f	na	v4	GTGCCAGCMGCCGCGGTAA
3	0	357f	na	v3	CCTACGGGNBGCWSCAG
4	0	27f	na	v1	AGAGTTTGATCMTGGYTCAG
\.


--
-- Name: primerstable_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dbbact
--

SELECT pg_catalog.setval('public.primerstable_id_seq', 1, true);


--
-- Name: primerstable_iduser_seq; Type: SEQUENCE SET; Schema: public; Owner: dbbact
--

SELECT pg_catalog.setval('public.primerstable_iduser_seq', 2, true);


--
-- Name: primerstable primerstable_pkey; Type: CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.primerstable
    ADD CONSTRAINT primerstable_pkey PRIMARY KEY (id);


--
-- Name: primerstable primerstable_iduser_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.primerstable
    ADD CONSTRAINT primerstable_iduser_fkey FOREIGN KEY (iduser) REFERENCES public.userstable(id) ON UPDATE SET NULL ON DELETE SET NULL;


--
-- PostgreSQL database dump complete
--

