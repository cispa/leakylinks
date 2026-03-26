CREATE FUNCTION public.create_analysis_output_entry() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO analysis_output (source_table, source_id, page_url, result_url, created_at)
    SELECT
        TG_TABLE_NAME::text,
        NEW.id,
        NEW.page_url,
        NEW.result_url,
        now()
    WHERE NOT EXISTS (
        SELECT 1 FROM analysis_output
        WHERE source_table = TG_TABLE_NAME::text
          AND source_id = NEW.id
    );
    RETURN NEW;
END;
$$;


--
-- Name: create_phase_entries(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.create_phase_entries() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO task_phase_status (source_table, source_id, phase, status)
    SELECT
        TG_TABLE_NAME::text,
        NEW.id,
        'crawl',
        'PENDING'
    WHERE NOT EXISTS (
        SELECT 1
        FROM task_phase_status
        WHERE source_table = TG_TABLE_NAME::text
        AND source_id = NEW.id
        AND phase = 'live_crawl'
    )
    ON CONFLICT (source_table, source_id, phase) DO NOTHING;

    RETURN NEW;
END;
$$;


--
-- Name: set_page_url_hash(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.set_page_url_hash() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.page_url_hash = MD5(NEW.page_url);
    RETURN NEW;
END;
$$;


--
-- Name: analysis_results_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.analysis_results_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: analysis_output; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.analysis_output (
    id integer DEFAULT nextval('public.analysis_results_id_seq'::regclass) NOT NULL,
    source_table text NOT NULL,
    source_id integer NOT NULL,
    page_url text NOT NULL,
    result_url text,
    not_base_domain boolean,
    live_crawl_analysis jsonb,
    live_crawl_updated_at timestamp without time zone,
    created_at timestamp without time zone,
    is_malicious boolean DEFAULT false,
    finalurlbefore_has_token boolean,
    has_redirection boolean,
    page_different boolean,
    CONSTRAINT analysis_output_id_check CHECK ((id IS NOT NULL)),
    CONSTRAINT analysis_output_page_url_check CHECK ((page_url IS NOT NULL)),
    CONSTRAINT analysis_output_source_id_check CHECK ((source_id IS NOT NULL)),
    CONSTRAINT analysis_output_source_table_check CHECK ((source_table IS NOT NULL))
);


--
-- Name: anyrun_results_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.anyrun_results_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: anyrun_results; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.anyrun_results (
    id integer DEFAULT nextval('public.anyrun_results_id_seq'::regclass) NOT NULL,
    method character varying,
    "time" timestamp without time zone,
    page_url text,
    result_url text,
    json_body jsonb,
    source text,
    page_url_hash text,
    CONSTRAINT anyrun_results_id_check CHECK ((id IS NOT NULL))
);


--
-- Name: cloudflare_results_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.cloudflare_results_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: cloudflare_results; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cloudflare_results (
    id integer DEFAULT nextval('public.cloudflare_results_id_seq'::regclass) NOT NULL,
    method character varying,
    "time" timestamp without time zone,
    page_url text,
    result_url text,
    source text,
    page_url_hash text,
    country character varying(4),
    is_malicious boolean,
    CONSTRAINT cloudflare_results_id_check CHECK ((id IS NOT NULL))
);


--
-- Name: hybrid_analysis_results_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.hybrid_analysis_results_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: hybrid_analysis_results; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.hybrid_analysis_results (
    id integer DEFAULT nextval('public.hybrid_analysis_results_id_seq'::regclass) NOT NULL,
    method character varying,
    "time" timestamp without time zone,
    page_url text,
    result_url text,
    source text,
    page_url_hash text,
    is_malicious text,
    CONSTRAINT hybrid_analysis_results_id_check CHECK ((id IS NOT NULL))
);


--
-- Name: joe_results_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.joe_results_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: joe_results; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.joe_results (
    id integer DEFAULT nextval('public.joe_results_id_seq'::regclass) NOT NULL,
    webid text,
    "time" timestamp without time zone,
    page_url text,
    result_url text,
    json_body jsonb,
    source text,
    method character varying,
    CONSTRAINT joe_results_id_check CHECK ((id IS NOT NULL))
);

--
-- Name: refined_output; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.refined_output (
    id integer NOT NULL,
    urlid bigint NOT NULL,
    service text NOT NULL,
    uuid text NOT NULL,
    additional_data jsonb,
    malicious boolean,
    refined_at text DEFAULT now() NOT NULL,
    refined_dir text
);


--
-- Name: refined_results_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.refined_results_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: refined_results_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.refined_results_id_seq OWNED BY public.refined_output.id;


--
-- Name: second_filter_artifact_status; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.second_filter_artifact_status (
    id integer NOT NULL,
    source_table text NOT NULL,
    source_id integer NOT NULL,
    artifact_type text NOT NULL,
    status text NOT NULL,
    error_message text,
    updated_at timestamp without time zone DEFAULT now(),
    processing_time double precision
);


--
-- Name: second_filter_artifact_status_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.second_filter_artifact_status_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: second_filter_artifact_status_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.second_filter_artifact_status_id_seq OWNED BY public.second_filter_artifact_status.id;


CREATE TABLE public.task_phase_status (
    source_table text NOT NULL,
    source_id integer NOT NULL,
    phase text NOT NULL,
    status text DEFAULT 'PENDING'::text,
    result boolean,
    error_message text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    CONSTRAINT task_phase_status_phase_check CHECK (((phase)::text = ANY (ARRAY['live_crawl'::text, 'url_token_check'::text, 'page_difference_check'::text, 'spi_detector'::text]))),
    CONSTRAINT task_phase_status_phase_check1 CHECK ((phase IS NOT NULL)),
    CONSTRAINT task_phase_status_source_id_check CHECK ((source_id IS NOT NULL)),
    CONSTRAINT task_phase_status_source_table_check CHECK ((source_table IS NOT NULL))
);




--
-- Name: urlquery_results_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.urlquery_results_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: urlquery_results; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.urlquery_results (
    id integer DEFAULT nextval('public.urlquery_results_id_seq'::regclass) NOT NULL,
    method character varying,
    "time" timestamp without time zone,
    page_url text,
    result_url text,
    source text,
    page_url_hash text,
    CONSTRAINT urlquery_results_id_check CHECK ((id IS NOT NULL))
);


--
-- Name: urlscan_results_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.urlscan_results_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: screenshot_analysis_results; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.screenshot_analysis_results (
    source_table text NOT NULL,
    source_id integer NOT NULL,
    screenshot_path text,
    page_url text,
    finalurlbefore text,
    sensitive boolean,
    score double precision,
    reasons text[],
    quoted_evidence text[],
    primary_intent text,
    confidence double precision,
    processing_time double precision,
    analysis_timestamp timestamp without time zone,
    error_message text,
    llm_raw jsonb,
    CONSTRAINT screenshot_analysis_results_pkey PRIMARY KEY (source_table, source_id)
);

--
-- Name: urlscan_results; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.urlscan_results (
    id integer DEFAULT nextval('public.urlscan_results_id_seq'::regclass) NOT NULL,
    method character varying,
    "time" timestamp without time zone,
    page_url text,
    result_url text,
    source character varying,
    page_url_hash text,
    CONSTRAINT urlscan_results_id_check CHECK ((id IS NOT NULL))
);

ALTER TABLE ONLY public.refined_output ALTER COLUMN id SET DEFAULT nextval('public.refined_results_id_seq'::regclass);

ALTER TABLE ONLY public.second_filter_artifact_status ALTER COLUMN id SET DEFAULT nextval('public.second_filter_artifact_status_id_seq'::regclass);

ALTER TABLE ONLY public.analysis_output
    ADD CONSTRAINT analysis_output_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.anyrun_results
    ADD CONSTRAINT anyrun_results_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.cloudflare_results
    ADD CONSTRAINT cloudflare_results_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.hybrid_analysis_results
    ADD CONSTRAINT hybrid_analysis_results_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.joe_results
    ADD CONSTRAINT joe_results_page_url_key UNIQUE (page_url);

ALTER TABLE ONLY public.joe_results
    ADD CONSTRAINT joe_results_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.refined_output
    ADD CONSTRAINT refined_results_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.second_filter_artifact_status
    ADD CONSTRAINT second_filter_artifact_status_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.task_phase_status
    ADD CONSTRAINT task_phase_status_pkey PRIMARY KEY (source_table, source_id, phase);

ALTER TABLE ONLY public.second_filter_artifact_status
    ADD CONSTRAINT unique_artifact UNIQUE (source_table, source_id, artifact_type);

ALTER TABLE ONLY public.urlquery_results
    ADD CONSTRAINT urlquery_results_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.urlscan_results
    ADD CONSTRAINT urlscan_results_pkey PRIMARY KEY (id);

CREATE INDEX analysis_output_source_table_source_id_idx ON public.analysis_output USING btree (source_table, source_id);
CREATE INDEX screenshot_analysis_results_source_table_source_id_idx ON public.screenshot_analysis_results USING btree (source_table, source_id);
CREATE INDEX screenshot_analysis_results_vision_sensitive_idx ON public.screenshot_analysis_results USING btree (vision_sensitive) WHERE vision_sensitive = true;
CREATE UNIQUE INDEX anyrun_results_page_url_hash_key ON public.anyrun_results USING btree (page_url_hash);
CREATE UNIQUE INDEX cloudflare_results_page_url_hash_key ON public.cloudflare_results USING btree (page_url_hash);
CREATE UNIQUE INDEX hybrid_analysis_results_page_url_hash_key ON public.hybrid_analysis_results USING btree (page_url_hash);
CREATE INDEX idx_refined_results_service_urlid_refinedat ON public.refined_output USING btree (service, urlid);
CREATE UNIQUE INDEX idx_urlquery_page_url_hash ON public.urlquery_results USING btree (page_url_hash);
CREATE UNIQUE INDEX unique_page_url_md5 ON public.analysis_output USING btree (md5(page_url));
CREATE UNIQUE INDEX urlscan_results_page_url_hash_key ON public.urlscan_results USING btree (page_url_hash);
CREATE TRIGGER before_insert_anyrun BEFORE INSERT OR UPDATE OF page_url ON public.anyrun_results FOR EACH ROW EXECUTE FUNCTION public.set_page_url_hash();
CREATE TRIGGER before_insert_cloudflare BEFORE INSERT OR UPDATE OF page_url ON public.cloudflare_results FOR EACH ROW EXECUTE FUNCTION public.set_page_url_hash();
CREATE TRIGGER before_insert_hybrid BEFORE INSERT OR UPDATE OF page_url ON public.hybrid_analysis_results FOR EACH ROW EXECUTE FUNCTION public.set_page_url_hash();
CREATE TRIGGER before_insert_urlquery BEFORE INSERT OR UPDATE OF page_url ON public.urlquery_results FOR EACH ROW EXECUTE FUNCTION public.set_page_url_hash();
CREATE TRIGGER before_insert_urlscan BEFORE INSERT OR UPDATE OF page_url ON public.urlscan_results FOR EACH ROW EXECUTE FUNCTION public.set_page_url_hash();


-- Triggers removed for simulation; replacement script will perform equivalent inserts manually 
-- CREATE TRIGGER create_analysis_output_trigger_anyrun AFTER INSERT ON public.anyrun_results FOR EACH ROW EXECUTE FUNCTION public.create_analysis_output_entry();
-- CREATE TRIGGER create_analysis_output_trigger_cloudflare AFTER INSERT ON public.cloudflare_results FOR EACH ROW EXECUTE FUNCTION public.create_analysis_output_entry();
-- CREATE TRIGGER create_analysis_output_trigger_hybrid_analysis AFTER INSERT ON public.hybrid_analysis_results FOR EACH ROW EXECUTE FUNCTION public.create_analysis_output_entry();
-- CREATE TRIGGER create_analysis_output_trigger_joe AFTER INSERT ON public.joe_results FOR EACH ROW EXECUTE FUNCTION public.create_analysis_output_entry();
-- CREATE TRIGGER create_analysis_output_trigger_urlquery AFTER INSERT ON public.urlquery_results FOR EACH ROW EXECUTE FUNCTION public.create_analysis_output_entry();
-- CREATE TRIGGER create_analysis_output_trigger_urlscan AFTER INSERT ON public.urlscan_results FOR EACH ROW EXECUTE FUNCTION public.create_analysis_output_entry();
-- CREATE TRIGGER create_phase_entries_trigger_anyrun AFTER INSERT ON public.anyrun_results FOR EACH ROW EXECUTE FUNCTION public.create_phase_entries();
-- CREATE TRIGGER create_phase_entries_trigger_cloudflare AFTER INSERT ON public.cloudflare_results FOR EACH ROW EXECUTE FUNCTION public.create_phase_entries();
-- CREATE TRIGGER create_phase_entries_trigger_hybrid_analysis AFTER INSERT ON public.hybrid_analysis_results FOR EACH ROW EXECUTE FUNCTION public.create_phase_entries();
-- CREATE TRIGGER create_phase_entries_trigger_joe AFTER INSERT ON public.joe_results FOR EACH ROW EXECUTE FUNCTION public.create_phase_entries();
-- CREATE TRIGGER create_phase_entries_trigger_urlquery AFTER INSERT ON public.urlquery_results FOR EACH ROW EXECUTE FUNCTION public.create_phase_entries();
-- CREATE TRIGGER create_phase_entries_trigger_urlscan AFTER INSERT ON public.urlscan_results FOR EACH ROW EXECUTE FUNCTION public.create_phase_entries();

