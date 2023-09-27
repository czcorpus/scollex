CREATE TABLE intercorp_v13ud_en_fcolls (
  id INT PRIMARY KEY AUTO_INCREMENT,
  lemma varchar NOT NULL,
  upos varchar NOT NULL,
  p_lemma varchar NOT NULL,
  p_upos varchar NOT NULL,
  deprel varchar NOT NULL,
  freq int DEFAULT NULL
);

CREATE TABLE intercorp_v13ud_en_parent_sums (
  id INT PRIMARY KEY AUTO_INCREMENT,
  p_lemma varchar NOT NULL,
  p_upos varchar NOT NULL,
  deprel varchar NOT NULL,
  freq int NOT NULL
);

CREATE TABLE intercorp_v13ud_en_child_sums (
  id INT PRIMARY KEY AUTO_INCREMENT,
  lemma varchar NOT NULL,
  upos varchar NOT NULL,
  deprel varchar NOT NULL,
  freq int NOT NULL
);

