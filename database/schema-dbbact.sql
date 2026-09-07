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

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: postgres
--

-- *not* creating schema, since initdb creates it


ALTER SCHEMA public OWNER TO postgres;

--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: agenttypestable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.agenttypestable (
    id integer NOT NULL,
    description text
);


ALTER TABLE public.agenttypestable OWNER TO dbbact;

--
-- Name: agenttypes_id_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.agenttypes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.agenttypes_id_seq OWNER TO dbbact;

--
-- Name: agenttypes_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.agenttypes_id_seq OWNED BY public.agenttypestable.id;


--
-- Name: annotationdetailstypestable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.annotationdetailstypestable (
    id integer NOT NULL,
    description text
);


ALTER TABLE public.annotationdetailstypestable OWNER TO dbbact;

--
-- Name: annotationdetailstypestable_id_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.annotationdetailstypestable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.annotationdetailstypestable_id_seq OWNER TO dbbact;

--
-- Name: annotationdetailstypestable_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.annotationdetailstypestable_id_seq OWNED BY public.annotationdetailstypestable.id;


--
-- Name: annotationflags_id_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.annotationflags_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.annotationflags_id_seq OWNER TO dbbact;

--
-- Name: annotationflagstable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.annotationflagstable (
    id integer DEFAULT nextval('public.annotationflags_id_seq'::regclass) NOT NULL,
    annotationid integer,
    userid integer DEFAULT 0,
    reason text,
    status text,
    response text,
    date date DEFAULT now()
);


ALTER TABLE public.annotationflagstable OWNER TO dbbact;

--
-- Name: annotationlisttable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.annotationlisttable (
    idannotation integer NOT NULL,
    idannotationdetail integer NOT NULL,
    idontology integer NOT NULL
);


ALTER TABLE public.annotationlisttable OWNER TO dbbact;

--
-- Name: annotationlisttable_idannotation_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.annotationlisttable_idannotation_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.annotationlisttable_idannotation_seq OWNER TO dbbact;

--
-- Name: annotationlisttable_idannotation_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.annotationlisttable_idannotation_seq OWNED BY public.annotationlisttable.idannotation;


--
-- Name: annotationlisttable_idannotationdetail_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.annotationlisttable_idannotationdetail_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.annotationlisttable_idannotationdetail_seq OWNER TO dbbact;

--
-- Name: annotationlisttable_idannotationdetail_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.annotationlisttable_idannotationdetail_seq OWNED BY public.annotationlisttable.idannotationdetail;


--
-- Name: annotationlisttable_idontology_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.annotationlisttable_idontology_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.annotationlisttable_idontology_seq OWNER TO dbbact;

--
-- Name: annotationlisttable_idontology_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.annotationlisttable_idontology_seq OWNED BY public.annotationlisttable.idontology;


--
-- Name: annotationparentstable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.annotationparentstable (
    idannotation integer NOT NULL,
    annotationdetail text,
    ontology text,
    term_id text
);


ALTER TABLE public.annotationparentstable OWNER TO dbbact;

--
-- Name: annotationstable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.annotationstable (
    id integer NOT NULL,
    idexp integer NOT NULL,
    iduser integer NOT NULL,
    idannotationtype integer NOT NULL,
    idmethod integer NOT NULL,
    addeddate date,
    description text,
    idagenttype integer NOT NULL,
    isprivate text,
    seqcount integer DEFAULT 0,
    primerid integer DEFAULT 0,
    review_status integer DEFAULT 0
);


ALTER TABLE public.annotationstable OWNER TO dbbact;

--
-- Name: annotationstable_id_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.annotationstable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.annotationstable_id_seq OWNER TO dbbact;

--
-- Name: annotationstable_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.annotationstable_id_seq OWNED BY public.annotationstable.id;


--
-- Name: annotationstable_idagenttype_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.annotationstable_idagenttype_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.annotationstable_idagenttype_seq OWNER TO dbbact;

--
-- Name: annotationstable_idagenttype_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.annotationstable_idagenttype_seq OWNED BY public.annotationstable.idagenttype;


--
-- Name: annotationstable_idannotationtype_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.annotationstable_idannotationtype_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.annotationstable_idannotationtype_seq OWNER TO dbbact;

--
-- Name: annotationstable_idannotationtype_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.annotationstable_idannotationtype_seq OWNED BY public.annotationstable.idannotationtype;


--
-- Name: annotationstable_idexp_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.annotationstable_idexp_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.annotationstable_idexp_seq OWNER TO dbbact;

--
-- Name: annotationstable_idexp_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.annotationstable_idexp_seq OWNED BY public.annotationstable.idexp;


--
-- Name: annotationstable_idmethod_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.annotationstable_idmethod_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.annotationstable_idmethod_seq OWNER TO dbbact;

--
-- Name: annotationstable_idmethod_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.annotationstable_idmethod_seq OWNED BY public.annotationstable.idmethod;


--
-- Name: annotationstable_iduser_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.annotationstable_iduser_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.annotationstable_iduser_seq OWNER TO dbbact;

--
-- Name: annotationstable_iduser_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.annotationstable_iduser_seq OWNED BY public.annotationstable.iduser;


--
-- Name: annotationtypestable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.annotationtypestable (
    id integer NOT NULL,
    description text
);


ALTER TABLE public.annotationtypestable OWNER TO dbbact;

--
-- Name: annotationtypestable_id_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.annotationtypestable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.annotationtypestable_id_seq OWNER TO dbbact;

--
-- Name: annotationtypestable_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.annotationtypestable_id_seq OWNED BY public.annotationtypestable.id;


--
-- Name: experimentstable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.experimentstable (
    uniqueid integer NOT NULL,
    expid integer NOT NULL,
    type text,
    value text,
    date date,
    userid integer NOT NULL,
    private text
);


ALTER TABLE public.experimentstable OWNER TO dbbact;

--
-- Name: experimentstable_expid_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.experimentstable_expid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.experimentstable_expid_seq OWNER TO dbbact;

--
-- Name: experimentstable_expid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.experimentstable_expid_seq OWNED BY public.experimentstable.expid;


--
-- Name: experimentstable_uniqueid_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.experimentstable_uniqueid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.experimentstable_uniqueid_seq OWNER TO dbbact;

--
-- Name: experimentstable_uniqueid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.experimentstable_uniqueid_seq OWNED BY public.experimentstable.uniqueid;


--
-- Name: experimentstable_userid_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.experimentstable_userid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.experimentstable_userid_seq OWNER TO dbbact;

--
-- Name: experimentstable_userid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.experimentstable_userid_seq OWNED BY public.experimentstable.userid;


--
-- Name: methodtypestable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.methodtypestable (
    id integer NOT NULL,
    description text
);


ALTER TABLE public.methodtypestable OWNER TO dbbact;

--
-- Name: methodtype_id_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.methodtype_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.methodtype_id_seq OWNER TO dbbact;

--
-- Name: methodtype_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.methodtype_id_seq OWNED BY public.methodtypestable.id;


--
-- Name: ontologynamestable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.ontologynamestable (
    id integer NOT NULL,
    description text
);


ALTER TABLE public.ontologynamestable OWNER TO dbbact;

--
-- Name: ontologynamestable_id_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.ontologynamestable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ontologynamestable_id_seq OWNER TO dbbact;

--
-- Name: ontologynamestable_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.ontologynamestable_id_seq OWNED BY public.ontologynamestable.id;


--
-- Name: ontologysynonymtable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.ontologysynonymtable (
    idontology integer NOT NULL,
    synonym text NOT NULL,
    uniqueid integer NOT NULL
);


ALTER TABLE public.ontologysynonymtable OWNER TO dbbact;

--
-- Name: ontologysynonymtable_idontology_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.ontologysynonymtable_idontology_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ontologysynonymtable_idontology_seq OWNER TO dbbact;

--
-- Name: ontologysynonymtable_idontology_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.ontologysynonymtable_idontology_seq OWNED BY public.ontologysynonymtable.idontology;


--
-- Name: ontologysynonymtable_uniqueid_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.ontologysynonymtable_uniqueid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ontologysynonymtable_uniqueid_seq OWNER TO dbbact;

--
-- Name: ontologysynonymtable_uniqueid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.ontologysynonymtable_uniqueid_seq OWNED BY public.ontologysynonymtable.uniqueid;


--
-- Name: ontologytable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.ontologytable (
    id integer NOT NULL,
    description text,
    exp_count integer DEFAULT 0,
    replaced_by integer DEFAULT 0,
    term_id text DEFAULT ''::text,
    seqcount integer DEFAULT 0,
    annotationcount integer DEFAULT 0,
    annotation_neg_count integer DEFAULT 0
);


ALTER TABLE public.ontologytable OWNER TO dbbact;

--
-- Name: ontologytable_id_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.ontologytable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ontologytable_id_seq OWNER TO dbbact;

--
-- Name: ontologytable_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.ontologytable_id_seq OWNED BY public.ontologytable.id;


--
-- Name: ontologytreestructuretable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.ontologytreestructuretable (
    ontologyid integer NOT NULL,
    ontologyparentid integer NOT NULL,
    ontologynameid integer NOT NULL,
    uniqueid integer NOT NULL
);


ALTER TABLE public.ontologytreestructuretable OWNER TO dbbact;

--
-- Name: ontologytreestructuretable_ontologynameid_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.ontologytreestructuretable_ontologynameid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ontologytreestructuretable_ontologynameid_seq OWNER TO dbbact;

--
-- Name: ontologytreestructuretable_ontologynameid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.ontologytreestructuretable_ontologynameid_seq OWNED BY public.ontologytreestructuretable.ontologynameid;


--
-- Name: ontologytreestructuretable_ontologyparentid_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.ontologytreestructuretable_ontologyparentid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ontologytreestructuretable_ontologyparentid_seq OWNER TO dbbact;

--
-- Name: ontologytreestructuretable_ontologyparentid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.ontologytreestructuretable_ontologyparentid_seq OWNED BY public.ontologytreestructuretable.ontologyparentid;


--
-- Name: ontologytreestructuretable_uniqueid_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.ontologytreestructuretable_uniqueid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ontologytreestructuretable_uniqueid_seq OWNER TO dbbact;

--
-- Name: ontologytreestructuretable_uniqueid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.ontologytreestructuretable_uniqueid_seq OWNED BY public.ontologytreestructuretable.uniqueid;


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
-- Name: sequencesannotationtable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.sequencesannotationtable (
    seqid integer NOT NULL,
    annotationid integer NOT NULL
);


ALTER TABLE public.sequencesannotationtable OWNER TO dbbact;

--
-- Name: sequencesannotationtable_annotationid_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.sequencesannotationtable_annotationid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.sequencesannotationtable_annotationid_seq OWNER TO dbbact;

--
-- Name: sequencesannotationtable_sequenceid_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.sequencesannotationtable_sequenceid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.sequencesannotationtable_sequenceid_seq OWNER TO dbbact;

--
-- Name: sequencesannotationtable_sequenceid_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.sequencesannotationtable_sequenceid_seq OWNED BY public.sequencesannotationtable.seqid;


--
-- Name: sequencestable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.sequencestable (
    id integer NOT NULL,
    idprimer integer NOT NULL,
    sequence text,
    length text,
    taxonomy text,
    ggid integer,
    seedsequence text,
    taxrootrank text,
    taxdomain text,
    taxphylum text,
    taxclass text,
    taxfamily text,
    taxgenus text,
    taxorder text,
    hashfull text,
    hash150 text,
    hash100 text,
    total_annotations integer DEFAULT 0,
    total_experiments integer DEFAULT 0
);


ALTER TABLE public.sequencestable OWNER TO dbbact;

--
-- Name: sequencestable_id_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.sequencestable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.sequencestable_id_seq OWNER TO dbbact;

--
-- Name: sequencestable_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.sequencestable_id_seq OWNED BY public.sequencestable.id;


--
-- Name: sequencestable_idprimer_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.sequencestable_idprimer_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.sequencestable_idprimer_seq OWNER TO dbbact;

--
-- Name: sequencestable_idprimer_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.sequencestable_idprimer_seq OWNED BY public.sequencestable.idprimer;


--
-- Name: terminfotable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.terminfotable (
    term text,
    totalexperiments integer,
    totalannotations integer,
    termtype text
);


ALTER TABLE public.terminfotable OWNER TO dbbact;

--
-- Name: termpairstable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.termpairstable (
    termpair text NOT NULL,
    annotationcount integer
);


ALTER TABLE public.termpairstable OWNER TO dbbact;

--
-- Name: usersprivatetable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.usersprivatetable (
    id integer NOT NULL,
    passwordhash text,
    name text,
    description text,
    isactive boolean,
    attemptscounter integer,
    email text,
    shareemail text,
    isadmin text,
    tempcodehash text,
    recoveryattemptscounter integer,
    username text
);


ALTER TABLE public.usersprivatetable OWNER TO dbbact;

--
-- Name: userstable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.userstable (
    id integer NOT NULL,
    username text
);


ALTER TABLE public.userstable OWNER TO dbbact;

--
-- Name: TABLE userstable; Type: COMMENT; Schema: public; Owner: dbbact
--

COMMENT ON TABLE public.userstable IS 'In this table we will store list of DB users';


--
-- Name: COLUMN userstable.id; Type: COMMENT; Schema: public; Owner: dbbact
--

COMMENT ON COLUMN public.userstable.id IS 'User id (auto increment)';


--
-- Name: userstable_id_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.userstable_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.userstable_id_seq OWNER TO dbbact;

--
-- Name: userstable_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.userstable_id_seq OWNED BY public.userstable.id;


--
-- Name: wholeSeqDatabaseTable_dbId_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public."wholeSeqDatabaseTable_dbId_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public."wholeSeqDatabaseTable_dbId_seq" OWNER TO dbbact;

--
-- Name: wholeseqdatabasetable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.wholeseqdatabasetable (
    dbid integer DEFAULT nextval('public."wholeSeqDatabaseTable_dbId_seq"'::regclass) NOT NULL,
    version text,
    dbname text
);


ALTER TABLE public.wholeseqdatabasetable OWNER TO dbbact;

--
-- Name: wholeseqidstable; Type: TABLE; Schema: public; Owner: dbbact
--

CREATE TABLE public.wholeseqidstable (
    id integer NOT NULL,
    dbid integer,
    dbbactid integer,
    wholeseqid text
)
WITH (autovacuum_vacuum_scale_factor='0.0', autovacuum_vacuum_threshold='5000', autovacuum_analyze_scale_factor='0.0', autovacuum_analyze_threshold='5000');


ALTER TABLE public.wholeseqidstable OWNER TO dbbact;

--
-- Name: wholeseqidstable_id_seq; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.wholeseqidstable_id_seq
    START WITH 1406909
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.wholeseqidstable_id_seq OWNER TO dbbact;

--
-- Name: wholeseqidstable_id_seq1; Type: SEQUENCE; Schema: public; Owner: dbbact
--

CREATE SEQUENCE public.wholeseqidstable_id_seq1
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.wholeseqidstable_id_seq1 OWNER TO dbbact;

--
-- Name: wholeseqidstable_id_seq1; Type: SEQUENCE OWNED BY; Schema: public; Owner: dbbact
--

ALTER SEQUENCE public.wholeseqidstable_id_seq1 OWNED BY public.wholeseqidstable.id;


--
-- Name: agenttypestable id; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.agenttypestable ALTER COLUMN id SET DEFAULT nextval('public.agenttypes_id_seq'::regclass);


--
-- Name: annotationdetailstypestable id; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationdetailstypestable ALTER COLUMN id SET DEFAULT nextval('public.annotationdetailstypestable_id_seq'::regclass);


--
-- Name: annotationlisttable idannotation; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationlisttable ALTER COLUMN idannotation SET DEFAULT nextval('public.annotationlisttable_idannotation_seq'::regclass);


--
-- Name: annotationlisttable idannotationdetail; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationlisttable ALTER COLUMN idannotationdetail SET DEFAULT nextval('public.annotationlisttable_idannotationdetail_seq'::regclass);


--
-- Name: annotationlisttable idontology; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationlisttable ALTER COLUMN idontology SET DEFAULT nextval('public.annotationlisttable_idontology_seq'::regclass);


--
-- Name: annotationstable id; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationstable ALTER COLUMN id SET DEFAULT nextval('public.annotationstable_id_seq'::regclass);


--
-- Name: annotationstable idexp; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationstable ALTER COLUMN idexp SET DEFAULT nextval('public.annotationstable_idexp_seq'::regclass);


--
-- Name: annotationstable iduser; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationstable ALTER COLUMN iduser SET DEFAULT nextval('public.annotationstable_iduser_seq'::regclass);


--
-- Name: annotationstable idannotationtype; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationstable ALTER COLUMN idannotationtype SET DEFAULT nextval('public.annotationstable_idannotationtype_seq'::regclass);


--
-- Name: annotationstable idmethod; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationstable ALTER COLUMN idmethod SET DEFAULT nextval('public.annotationstable_idmethod_seq'::regclass);


--
-- Name: annotationstable idagenttype; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationstable ALTER COLUMN idagenttype SET DEFAULT nextval('public.annotationstable_idagenttype_seq'::regclass);


--
-- Name: annotationtypestable id; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationtypestable ALTER COLUMN id SET DEFAULT nextval('public.annotationtypestable_id_seq'::regclass);


--
-- Name: experimentstable uniqueid; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.experimentstable ALTER COLUMN uniqueid SET DEFAULT nextval('public.experimentstable_uniqueid_seq'::regclass);


--
-- Name: experimentstable expid; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.experimentstable ALTER COLUMN expid SET DEFAULT nextval('public.experimentstable_expid_seq'::regclass);


--
-- Name: experimentstable userid; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.experimentstable ALTER COLUMN userid SET DEFAULT nextval('public.experimentstable_userid_seq'::regclass);


--
-- Name: methodtypestable id; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.methodtypestable ALTER COLUMN id SET DEFAULT nextval('public.methodtype_id_seq'::regclass);


--
-- Name: ontologynamestable id; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.ontologynamestable ALTER COLUMN id SET DEFAULT nextval('public.ontologynamestable_id_seq'::regclass);


--
-- Name: ontologysynonymtable idontology; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.ontologysynonymtable ALTER COLUMN idontology SET DEFAULT nextval('public.ontologysynonymtable_idontology_seq'::regclass);


--
-- Name: ontologysynonymtable uniqueid; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.ontologysynonymtable ALTER COLUMN uniqueid SET DEFAULT nextval('public.ontologysynonymtable_uniqueid_seq'::regclass);


--
-- Name: ontologytable id; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.ontologytable ALTER COLUMN id SET DEFAULT nextval('public.ontologytable_id_seq'::regclass);


--
-- Name: ontologytreestructuretable ontologyparentid; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.ontologytreestructuretable ALTER COLUMN ontologyparentid SET DEFAULT nextval('public.ontologytreestructuretable_ontologyparentid_seq'::regclass);


--
-- Name: ontologytreestructuretable ontologynameid; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.ontologytreestructuretable ALTER COLUMN ontologynameid SET DEFAULT nextval('public.ontologytreestructuretable_ontologynameid_seq'::regclass);


--
-- Name: ontologytreestructuretable uniqueid; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.ontologytreestructuretable ALTER COLUMN uniqueid SET DEFAULT nextval('public.ontologytreestructuretable_uniqueid_seq'::regclass);


--
-- Name: primerstable id; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.primerstable ALTER COLUMN id SET DEFAULT nextval('public.primerstable_id_seq'::regclass);


--
-- Name: primerstable iduser; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.primerstable ALTER COLUMN iduser SET DEFAULT nextval('public.primerstable_iduser_seq'::regclass);


--
-- Name: sequencesannotationtable seqid; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.sequencesannotationtable ALTER COLUMN seqid SET DEFAULT nextval('public.sequencesannotationtable_sequenceid_seq'::regclass);


--
-- Name: sequencestable id; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.sequencestable ALTER COLUMN id SET DEFAULT nextval('public.sequencestable_id_seq'::regclass);


--
-- Name: sequencestable idprimer; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.sequencestable ALTER COLUMN idprimer SET DEFAULT nextval('public.sequencestable_idprimer_seq'::regclass);


--
-- Name: userstable id; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.userstable ALTER COLUMN id SET DEFAULT nextval('public.userstable_id_seq'::regclass);


--
-- Name: wholeseqidstable id; Type: DEFAULT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.wholeseqidstable ALTER COLUMN id SET DEFAULT nextval('public.wholeseqidstable_id_seq1'::regclass);


--
-- Name: agenttypestable agenttypes_pkey; Type: CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.agenttypestable
    ADD CONSTRAINT agenttypes_pkey PRIMARY KEY (id);


--
-- Name: annotationdetailstypestable annotationdetailstypestable_pkey; Type: CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationdetailstypestable
    ADD CONSTRAINT annotationdetailstypestable_pkey PRIMARY KEY (id);


--
-- Name: annotationflagstable annotationflagstable_pkey; Type: CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationflagstable
    ADD CONSTRAINT annotationflagstable_pkey PRIMARY KEY (id);


--
-- Name: annotationlisttable annotationlisttable_pkey; Type: CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationlisttable
    ADD CONSTRAINT annotationlisttable_pkey PRIMARY KEY (idannotation, idannotationdetail, idontology);


--
-- Name: annotationstable annotationstable_pkey; Type: CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationstable
    ADD CONSTRAINT annotationstable_pkey PRIMARY KEY (id);


--
-- Name: annotationtypestable annotationtypestable_pkey; Type: CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationtypestable
    ADD CONSTRAINT annotationtypestable_pkey PRIMARY KEY (id);


--
-- Name: experimentstable experimentstable_pkey; Type: CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.experimentstable
    ADD CONSTRAINT experimentstable_pkey PRIMARY KEY (uniqueid);


--
-- Name: methodtypestable methodtype_pkey; Type: CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.methodtypestable
    ADD CONSTRAINT methodtype_pkey PRIMARY KEY (id);


--
-- Name: ontologynamestable ontologynamestable_pkey; Type: CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.ontologynamestable
    ADD CONSTRAINT ontologynamestable_pkey PRIMARY KEY (id);


--
-- Name: ontologytreestructuretable ontologytreestructuretable_pkey; Type: CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.ontologytreestructuretable
    ADD CONSTRAINT ontologytreestructuretable_pkey PRIMARY KEY (ontologyid, ontologyparentid, ontologynameid);


--
-- Name: primerstable primerstable_pkey; Type: CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.primerstable
    ADD CONSTRAINT primerstable_pkey PRIMARY KEY (id);


--
-- Name: sequencestable sequencestable_pkey; Type: CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.sequencestable
    ADD CONSTRAINT sequencestable_pkey PRIMARY KEY (id);


--
-- Name: termpairstable termpairstable_pkey; Type: CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.termpairstable
    ADD CONSTRAINT termpairstable_pkey PRIMARY KEY (termpair);


--
-- Name: usersprivatetable usersprivatetable_pkey; Type: CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.usersprivatetable
    ADD CONSTRAINT usersprivatetable_pkey PRIMARY KEY (id);


--
-- Name: userstable userstable_pkey; Type: CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.userstable
    ADD CONSTRAINT userstable_pkey PRIMARY KEY (id);


--
-- Name: userstable userstable_username_key; Type: CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.userstable
    ADD CONSTRAINT userstable_username_key UNIQUE (username);


--
-- Name: wholeseqdatabasetable wholeSeqDatabaseTable_pkey; Type: CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.wholeseqdatabasetable
    ADD CONSTRAINT "wholeSeqDatabaseTable_pkey" PRIMARY KEY (dbid);


--
-- Name: wholeseqidstable wholeseqidstable_pkey; Type: CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.wholeseqidstable
    ADD CONSTRAINT wholeseqidstable_pkey PRIMARY KEY (id);


--
-- Name: agenttypestable_description_idx; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX agenttypestable_description_idx ON public.agenttypestable USING btree (description);


--
-- Name: annotationdetailstypestable_description_index; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX annotationdetailstypestable_description_index ON public.annotationdetailstypestable USING btree (description);


--
-- Name: annotationflags_pkey; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE UNIQUE INDEX annotationflags_pkey ON public.annotationflagstable USING btree (id);


--
-- Name: annotationflags_status_idx; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX annotationflags_status_idx ON public.annotationflagstable USING btree (status);


--
-- Name: annotationflags_userid_idx; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX annotationflags_userid_idx ON public.annotationflagstable USING btree (userid);


--
-- Name: annotationid_index; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX annotationid_index ON public.sequencesannotationtable USING btree (annotationid);


--
-- Name: annotationlisttable_idannotation_idx; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX annotationlisttable_idannotation_idx ON public.annotationlisttable USING btree (idannotation);


--
-- Name: annotationlisttable_idontology_index; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX annotationlisttable_idontology_index ON public.annotationlisttable USING btree (idontology);


--
-- Name: annotationparentstable_idannotation_idx; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX annotationparentstable_idannotation_idx ON public.annotationparentstable USING btree (idannotation);


--
-- Name: annotationparentstable_ontology_idx; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX annotationparentstable_ontology_idx ON public.annotationparentstable USING btree (ontology);


--
-- Name: annotationparentstable_term_id_idx; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX annotationparentstable_term_id_idx ON public.annotationparentstable USING btree (term_id);


--
-- Name: annotationstable_idexp_index; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX annotationstable_idexp_index ON public.annotationstable USING btree (idexp);


--
-- Name: annotationstable_iduser_index; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX annotationstable_iduser_index ON public.annotationstable USING btree (iduser);


--
-- Name: annotationtypestable_description_index; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX annotationtypestable_description_index ON public.annotationtypestable USING btree (description);


--
-- Name: experimentstable_expid_index; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX experimentstable_expid_index ON public.experimentstable USING btree (expid);


--
-- Name: experimentstable_value_index; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX experimentstable_value_index ON public.experimentstable USING btree (value);


--
-- Name: external_db_id_index; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX external_db_id_index ON public.wholeseqidstable USING btree (wholeseqid);


--
-- Name: methodtypestable_description_index; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX methodtypestable_description_index ON public.methodtypestable USING btree (description);


--
-- Name: ontologysynonymtable _synonym_index; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX "ontologysynonymtable _synonym_index" ON public.ontologysynonymtable USING btree (synonym);


--
-- Name: ontologysynonymtable_idontology_idx; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX ontologysynonymtable_idontology_idx ON public.ontologysynonymtable USING btree (idontology);


--
-- Name: ontologytable_description_index; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX ontologytable_description_index ON public.ontologytable USING btree (description);


--
-- Name: ontologytable_pkey; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE UNIQUE INDEX ontologytable_pkey ON public.ontologytable USING btree (id);


--
-- Name: ontologytreestructuretable_ontologyid_idx; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX ontologytreestructuretable_ontologyid_idx ON public.ontologytreestructuretable USING btree (ontologyid);


--
-- Name: ontologytreestructuretable_ontologyparentid_index; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX ontologytreestructuretable_ontologyparentid_index ON public.ontologytreestructuretable USING btree (ontologyparentid);


--
-- Name: ontologytreestructuretable_uniqueid_index; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX ontologytreestructuretable_uniqueid_index ON public.ontologytreestructuretable USING btree (uniqueid);


--
-- Name: seqid_index; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX seqid_index ON public.sequencesannotationtable USING btree (seqid);


--
-- Name: sequencesannotationtable_seqid_annotationid_idx; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX sequencesannotationtable_seqid_annotationid_idx ON public.sequencesannotationtable USING btree (seqid, annotationid);


--
-- Name: sequencestable_ggid_index; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX sequencestable_ggid_index ON public.sequencestable USING btree (ggid);


--
-- Name: sequencestable_hash100_idx; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX sequencestable_hash100_idx ON public.sequencestable USING btree (hash100);


--
-- Name: sequencestable_hash150_idx; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX sequencestable_hash150_idx ON public.sequencestable USING btree (hash150);


--
-- Name: sequencestable_hashfull_idx; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX sequencestable_hashfull_idx ON public.sequencestable USING btree (hashfull);


--
-- Name: sequencestable_seedsequence_index; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX sequencestable_seedsequence_index ON public.sequencestable USING btree (seedsequence);


--
-- Name: sequencestable_sequence_idx; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX sequencestable_sequence_idx ON public.sequencestable USING btree (sequence);


--
-- Name: sequencestable_taxonomy_idx; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX sequencestable_taxonomy_idx ON public.sequencestable USING btree (taxonomy);


--
-- Name: term_id_index; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX term_id_index ON public.ontologytable USING btree (term_id);


--
-- Name: terminfotable_term_idx; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX terminfotable_term_idx ON public.terminfotable USING btree (term);


--
-- Name: terminfotable_termtype_idx; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX terminfotable_termtype_idx ON public.terminfotable USING btree (termtype);


--
-- Name: trgm_idx; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX trgm_idx ON public.sequencestable USING gin (sequence public.gin_trgm_ops);


--
-- Name: wholeseqidstable_dbbactid_index; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX wholeseqidstable_dbbactid_index ON public.wholeseqidstable USING btree (dbbactid);


--
-- Name: wholeseqidstable_dbid_dbbactid_wholeseqid_idx; Type: INDEX; Schema: public; Owner: dbbact
--

CREATE INDEX wholeseqidstable_dbid_dbbactid_wholeseqid_idx ON public.wholeseqidstable USING btree (dbid, dbbactid, wholeseqid);


--
-- Name: annotationflagstable annotationflagstable_annotationid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationflagstable
    ADD CONSTRAINT annotationflagstable_annotationid_fkey FOREIGN KEY (annotationid) REFERENCES public.annotationstable(id) ON DELETE CASCADE;


--
-- Name: annotationflagstable annotationflagstable_userid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationflagstable
    ADD CONSTRAINT annotationflagstable_userid_fkey FOREIGN KEY (userid) REFERENCES public.userstable(id) ON DELETE SET DEFAULT;


--
-- Name: annotationlisttable annotationlisttable_idannotation_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationlisttable
    ADD CONSTRAINT annotationlisttable_idannotation_fkey FOREIGN KEY (idannotation) REFERENCES public.annotationstable(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: annotationlisttable annotationlisttable_idannotationdetail_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationlisttable
    ADD CONSTRAINT annotationlisttable_idannotationdetail_fkey FOREIGN KEY (idannotationdetail) REFERENCES public.annotationdetailstypestable(id);


--
-- Name: annotationlisttable annotationlisttable_idontology_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationlisttable
    ADD CONSTRAINT annotationlisttable_idontology_fkey FOREIGN KEY (idontology) REFERENCES public.ontologytable(id);


--
-- Name: annotationparentstable annotationparentstable_idannotation_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationparentstable
    ADD CONSTRAINT annotationparentstable_idannotation_fkey FOREIGN KEY (idannotation) REFERENCES public.annotationstable(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: annotationstable annotationstable_idagenttype_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationstable
    ADD CONSTRAINT annotationstable_idagenttype_fkey FOREIGN KEY (idagenttype) REFERENCES public.agenttypestable(id);


--
-- Name: annotationstable annotationstable_idannotationtype_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationstable
    ADD CONSTRAINT annotationstable_idannotationtype_fkey FOREIGN KEY (idannotationtype) REFERENCES public.annotationtypestable(id);


--
-- Name: annotationstable annotationstable_idmethod_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationstable
    ADD CONSTRAINT annotationstable_idmethod_fkey FOREIGN KEY (idmethod) REFERENCES public.methodtypestable(id);


--
-- Name: annotationstable annotationstable_iduser_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationstable
    ADD CONSTRAINT annotationstable_iduser_fkey FOREIGN KEY (iduser) REFERENCES public.userstable(id) ON UPDATE SET NULL ON DELETE SET DEFAULT;


--
-- Name: annotationstable annotationstable_primerid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.annotationstable
    ADD CONSTRAINT annotationstable_primerid_fkey FOREIGN KEY (primerid) REFERENCES public.primerstable(id);


--
-- Name: experimentstable experimentstable_userid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.experimentstable
    ADD CONSTRAINT experimentstable_userid_fkey FOREIGN KEY (userid) REFERENCES public.userstable(id) ON UPDATE SET NULL ON DELETE SET NULL;


--
-- Name: ontologysynonymtable ontologysynonymtable_idontology_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.ontologysynonymtable
    ADD CONSTRAINT ontologysynonymtable_idontology_fkey FOREIGN KEY (idontology) REFERENCES public.ontologytable(id);


--
-- Name: ontologytreestructuretable ontologytreestructuretable_ontologyid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.ontologytreestructuretable
    ADD CONSTRAINT ontologytreestructuretable_ontologyid_fkey FOREIGN KEY (ontologyid) REFERENCES public.ontologytable(id);


--
-- Name: ontologytreestructuretable ontologytreestructuretable_ontologynameid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.ontologytreestructuretable
    ADD CONSTRAINT ontologytreestructuretable_ontologynameid_fkey FOREIGN KEY (ontologynameid) REFERENCES public.ontologynamestable(id);


--
-- Name: ontologytreestructuretable ontologytreestructuretable_ontologyparentid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.ontologytreestructuretable
    ADD CONSTRAINT ontologytreestructuretable_ontologyparentid_fkey FOREIGN KEY (ontologyparentid) REFERENCES public.ontologytable(id);


--
-- Name: primerstable primerstable_iduser_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.primerstable
    ADD CONSTRAINT primerstable_iduser_fkey FOREIGN KEY (iduser) REFERENCES public.userstable(id) ON UPDATE SET NULL ON DELETE SET NULL;


--
-- Name: sequencesannotationtable sequencesannotationtable_annotationid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.sequencesannotationtable
    ADD CONSTRAINT sequencesannotationtable_annotationid_fkey FOREIGN KEY (annotationid) REFERENCES public.annotationstable(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: sequencesannotationtable sequencesannotationtable_sequenceid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.sequencesannotationtable
    ADD CONSTRAINT sequencesannotationtable_sequenceid_fkey FOREIGN KEY (seqid) REFERENCES public.sequencestable(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: sequencestable sequencestable_idprimer_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.sequencestable
    ADD CONSTRAINT sequencestable_idprimer_fkey FOREIGN KEY (idprimer) REFERENCES public.primerstable(id);


--
-- Name: usersprivatetable usersprivatetable_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.usersprivatetable
    ADD CONSTRAINT usersprivatetable_id_fkey FOREIGN KEY (id) REFERENCES public.userstable(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: usersprivatetable usersprivatetable_username_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.usersprivatetable
    ADD CONSTRAINT usersprivatetable_username_fkey FOREIGN KEY (username) REFERENCES public.userstable(username) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: wholeseqidstable wholeseqidstable_dbbactid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.wholeseqidstable
    ADD CONSTRAINT wholeseqidstable_dbbactid_fkey FOREIGN KEY (dbbactid) REFERENCES public.sequencestable(id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: wholeseqidstable wholeseqidstable_dbid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: dbbact
--

ALTER TABLE ONLY public.wholeseqidstable
    ADD CONSTRAINT wholeseqidstable_dbid_fkey FOREIGN KEY (dbid) REFERENCES public.wholeseqdatabasetable(dbid) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE USAGE ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

