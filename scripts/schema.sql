CREATE TABLE intercorp_v13ud_cs_fcolls (
  id SERIAL PRIMARY KEY,
  lemma varchar DEFAULT NULL,
  upos varchar DEFAULT NULL,
  p_lemma varchar DEFAULT NULL,
  p_upos varchar DEFAULT NULL,
  deprel varchar DEFAULT NULL,
  freq int DEFAULT NULL,
  chunk int DEFAULT NULL
);

CREATE TABLE intercorp_v13ud_en_fcolls (
  id SERIAL PRIMARY KEY,
  lemma varchar DEFAULT NULL,
  upos varchar DEFAULT NULL,
  p_lemma varchar DEFAULT NULL,
  p_upos varchar DEFAULT NULL,
  deprel varchar DEFAULT NULL,
  freq int DEFAULT NULL,
  chunk int DEFAULT NULL
);

CREATE MATERIALIZED VIEW intercorp_v13ud_cs_p_lemma_candidates AS
SELECT lemma, upos, p_lemma, p_upos, deprel, freq,
(SELECT SUM(freq) FROM intercorp_v13ud_cs_fcolls AS b
WHERE b.p_lemma = a.p_lemma AND b.p_upos = a.p_upos AND b.deprel = a.deprel) AS fy
FROM intercorp_v13ud_cs_fcolls AS a;


CREATE MATERIALIZED VIEW intercorp_v13ud_cs_lemma_candidates
AS SELECT a.lemma, a.upos, p_lemma, p_upos, deprel, a.freq,
(SELECT SUM(freq) FROM intercorp_v13ud_cs_fcolls AS b
WHERE b.lemma = a.lemma AND b.upos = a.upos AND b.deprel = a.deprel) AS fy
FROM intercorp_v13ud_cs_fcolls AS a;