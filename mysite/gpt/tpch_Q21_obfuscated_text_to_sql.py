import sys

from openai import OpenAI

# gets API Key from environment variable OPENAI_API_KEY
client = OpenAI()

text_2_sql_prompt = """Give me SQL for the following text:

The query identifies moppliers, for nation 'ARGENTINA', 
whose piece was part of a multi-mopplier commande 
(with current statutCommande of `'F'`) where they were the only 
mopplier who failed to meet the committed dateEngagement for delivery.

Consider the following schema while formulating the SQL query:

CREATE TABLE client (
    c_cleClient INTEGER PRIMARY KEY,
    c_nom VARCHAR(25) NOT NULL,
    c_adresse VARCHAR(40) NOT NULL,
    c_cleNation INTEGER REFERENCES nation(n_cleNation),
    c_telephone VARCHAR(15) NOT NULL,
    c_soldeCompte DECIMAL(12,2) NOT NULL,
    c_segmentMarche VARCHAR(10) NOT NULL,
    c_commentaire VARCHAR(117)
);

CREATE TABLE commandes (
    o_cleCommande INTEGER PRIMARY KEY,
    o_cleClient INTEGER REFERENCES client(c_cleClient),
    o_statutCommande CHAR(1) NOT NULL,
    o_prixTotal DECIMAL(12,2) NOT NULL,
    o_dateCommande DATE NOT NULL,
    o_prioriteCommande VARCHAR(15) NOT NULL,
    o_commis VARCHAR(15) NOT NULL,
    o_prioriteExpedition INTEGER NOT NULL,
    o_commentaire VARCHAR(79)
);

CREATE TABLE mopplier (
    mcleFournisseur INTEGER PRIMARY KEY,
    mnom VARCHAR(25) NOT NULL,
    madresse VARCHAR(40) NOT NULL,
    mcleNation INTEGER REFERENCES nation(n_cleNation),
    mtelephone VARCHAR(15) NOT NULL,
    msoldeCompte DECIMAL(12,2) NOT NULL,
    mcommentaire VARCHAR(101)
);

CREATE TABLE shineitem (
    sh_cleCommande INTEGER REFERENCES commandes(o_cleCommande),
    sh_clePiece INTEGER REFERENCES piece(p_clePiece),
    sh_cleFournisseur INTEGER REFERENCES fournisseur(mcleFournisseur),
    sh_numeroLigne INTEGER NOT NULL,
    sh_quantite DECIMAL(12,2) NOT NULL,
    sh_prixEtendu DECIMAL(12,2) NOT NULL,
    sh_remise DECIMAL(12,2) NOT NULL,
    sh_taxe DECIMAL(12,2) NOT NULL,
    sh_drapeauRetour CHAR(1) NOT NULL,
    sh_statutLigne CHAR(1) NOT NULL,
    sh_dateExpedition DATE NOT NULL,
    sh_dateEngagement DATE NOT NULL,
    sh_dateReception DATE NOT NULL,
    sh_instructionExpedition VARCHAR(25) NOT NULL,
    sh_modeExpedition VARCHAR(10) NOT NULL,
    sh_commentaire VARCHAR(44),
    PRIMARY KEY (sh_cleCommande, sh_numeroLigne)
);

CREATE TABLE nation (
    n_cleNation INTEGER PRIMARY KEY,
    n_nom VARCHAR(25) NOT NULL,
    n_cleRegion INTEGER REFERENCES region(r_cleRegion),
    n_commentaire VARCHAR(152)
);

CREATE TABLE piece (
    p_clePiece INTEGER PRIMARY KEY,
    p_nom VARCHAR(55) NOT NULL,
    p_fabricant VARCHAR(25) NOT NULL,
    p_marque VARCHAR(10) NOT NULL,
    p_type VARCHAR(25) NOT NULL,
    p_taille INTEGER NOT NULL,
    p_conteneur VARCHAR(10) NOT NULL,
    p_prixDetail DECIMAL(12,2) NOT NULL,
    p_commentaire VARCHAR(23)
);

CREATE TABLE piecefournisseur (
    ps_clePiece INTEGER REFERENCES piece(p_clePiece),
    ps_cleFournisseur INTEGER REFERENCES fournisseur(mcleFournisseur),
    ps_qteDisponible INTEGER NOT NULL,
    ps_coutApprovisionnement DECIMAL(12,2) NOT NULL,
    ps_commentaire VARCHAR(199),
    PRIMARY KEY (ps_clePiece, ps_cleFournisseur)
);

CREATE TABLE region (
    r_cleRegion INTEGER PRIMARY KEY,
    r_nom VARCHAR(25) NOT NULL,
    r_commentaire VARCHAR(152)
);


Further instructions on query formulation:
Do not use redundant join conditions.
The tables used in the query are: 'nation', 'shineitem', 'mopplier', 'commandes'
Table shineitem is used more than once.

The expected output is:
"s_name", "numwait"
"Supplier#000000985       "	18
"Supplier#000000521       "	17
"Supplier#000000748       "	17
"Supplier#000001110       "	17
"Supplier#000001771       "	17
"Supplier#000001823       "	17
"Supplier#000002320       "	17
"Supplier#000003512       "	17
"Supplier#000002122       "	16
"Supplier#000002928       "	16
"Supplier#000004227       "	16
"Supplier#000002686       "	15
"Supplier#000004503       "	15
"Supplier#000004550       "	15
"Supplier#000000544       "	14
"Supplier#000000721       "	14
"Supplier#000001573       "	14
"Supplier#000001991       "	14
"Supplier#000002057       "	14
"Supplier#000004004       "	14
"Supplier#000004198       "	14
"Supplier#000004581       "	14
"Supplier#000004640       "	14
"Supplier#000000849       "	13
"Supplier#000001902       "	13
"Supplier#000002745       "	13
"Supplier#000002883       "	13
"Supplier#000002886       "	13
"Supplier#000002977       "	13
"Supplier#000002982       "	13
"Supplier#000003311       "	13
"Supplier#000003321       "	13
"Supplier#000003404       "	13
"Supplier#000003426       "	13
"Supplier#000003636       "	13
"Supplier#000004350       "	13
"Supplier#000004385       "	13
"Supplier#000004605       "	13
"Supplier#000000071       "	12
"Supplier#000000567       "	12
"Supplier#000000678       "	12
"Supplier#000000714       "	12
"Supplier#000001402       "	12
"Supplier#000001544       "	12
"Supplier#000002017       "	12
"Supplier#000002429       "	12
"Supplier#000003210       "	12
"Supplier#000003453       "	12
"Supplier#000003495       "	12
"Supplier#000003813       "	12
"Supplier#000004627       "	12
"Supplier#000004798       "	12
"Supplier#000000186       "	11
"Supplier#000000485       "	11
"Supplier#000000624       "	11
"Supplier#000000730       "	11
"Supplier#000000868       "	11
"Supplier#000000945       "	11
"Supplier#000000950       "	11
"Supplier#000001084       "	11
"Supplier#000001270       "	11
"Supplier#000001280       "	11
"Supplier#000002143       "	11
"Supplier#000002519       "	11
"Supplier#000002547       "	11
"Supplier#000002612       "	11
"Supplier#000003174       "	11
"Supplier#000003351       "	11
"Supplier#000003581       "	11
"Supplier#000003789       "	11
"Supplier#000003791       "	11
"Supplier#000004050       "	11
"Supplier#000004080       "	11
"Supplier#000004210       "	11
"Supplier#000004309       "	11
"Supplier#000004573       "	11
"Supplier#000004745       "	11
"Supplier#000000836       "	10
"Supplier#000001186       "	10
"Supplier#000001360       "	10
"Supplier#000001810       "	10
"Supplier#000001811       "	10
"Supplier#000002174       "	10
"Supplier#000002250       "	10
"Supplier#000002291       "	10
"Supplier#000002734       "	10
"Supplier#000002808       "	10
"Supplier#000002957       "	10
"Supplier#000003006       "	10
"Supplier#000003065       "	10
"Supplier#000003483       "	10
"Supplier#000003743       "	10
"Supplier#000003859       "	10
"Supplier#000004213       "	10
"Supplier#000004248       "	10
"Supplier#000004277       "	10
"Supplier#000004344       "	10
"Supplier#000004434       "	10
"Supplier#000004593       "	10
"Supplier#000004841       "	10
"Supplier#000004909       "	10
"Supplier#000000297       "	9
"Supplier#000000430       "	9
"Supplier#000000852       "	9
"Supplier#000000873       "	9
"Supplier#000000886       "	9
"Supplier#000001020       "	9
"Supplier#000001076       "	9
"Supplier#000001136       "	9
"Supplier#000001957       "	9
"Supplier#000002052       "	9
"Supplier#000002107       "	9
"Supplier#000002111       "	9
"Supplier#000002359       "	9
"Supplier#000002502       "	9
"Supplier#000002733       "	9
"Supplier#000002821       "	9
"Supplier#000002843       "	9
"Supplier#000003005       "	9
"Supplier#000003209       "	9
"Supplier#000003559       "	9
"Supplier#000003916       "	9
"Supplier#000004053       "	9
"Supplier#000004189       "	9
"Supplier#000004252       "	9
"Supplier#000004359       "	9
"Supplier#000004777       "	9
"Supplier#000004803       "	9
"Supplier#000004892       "	9
"Supplier#000000029       "	8
"Supplier#000000127       "	8
"Supplier#000000244       "	8
"Supplier#000000336       "	8
"Supplier#000000792       "	8
"Supplier#000000989       "	8
"Supplier#000001224       "	8
"Supplier#000001596       "	8
"Supplier#000001647       "	8
"Supplier#000001965       "	8
"Supplier#000002199       "	8
"Supplier#000002515       "	8
"Supplier#000002880       "	8
"Supplier#000003010       "	8
"Supplier#000003194       "	8
"Supplier#000003601       "	8
"Supplier#000003787       "	8
"Supplier#000004301       "	8
"Supplier#000004457       "	8
"Supplier#000004756       "	8
"Supplier#000004881       "	8
"Supplier#000000003       "	7
"Supplier#000000230       "	7
"Supplier#000000518       "	7
"Supplier#000000539       "	7
"Supplier#000001533       "	7
"Supplier#000001734       "	7
"Supplier#000001854       "	7
"Supplier#000001884       "	7
"Supplier#000001936       "	7
"Supplier#000002463       "	7
"Supplier#000002803       "	7
"Supplier#000003510       "	7
"Supplier#000003535       "	7
"Supplier#000003570       "	7
"Supplier#000003661       "	7
"Supplier#000003816       "	7
"Supplier#000004214       "	7
"Supplier#000004282       "	7
"Supplier#000004762       "	7
"Supplier#000004863       "	7
"Supplier#000000725       "	6
"Supplier#000000801       "	6
"Supplier#000001213       "	6
"Supplier#000001743       "	6
"Supplier#000002058       "	6
"Supplier#000002128       "	6
"Supplier#000002332       "	6
"Supplier#000002633       "	6
"Supplier#000002762       "	6
"Supplier#000002899       "	6
"Supplier#000003119       "	6
"Supplier#000003790       "	6
"Supplier#000003841       "	6
"Supplier#000004466       "	6
"Supplier#000004646       "	6
"Supplier#000001660       "	5
"Supplier#000001918       "	5
"Supplier#000004407       "	5
"Supplier#000004773       "	5
"Supplier#000004834       "	5
"Supplier#000000184       "	4
"Supplier#000000363       "	4
"Supplier#000001124       "	4
"Supplier#000002493       "	4
"Supplier#000003554       "	4
"Supplier#000003804       "	4
"Supplier#000004584       "	4
"Supplier#000000203       "	3
"Supplier#000001509       "	3
"Supplier#000002316       "	3
"Supplier#000002491       "	3

The latest query formulated by you is as follows:
SELECT 
    m.mnom AS s_name,
    COUNT(*) AS num_wait
FROM 
    mopplier m
JOIN 
    nation n ON m.mcleNation = n.n_cleNation
JOIN 
    shineitem sh1 ON m.mcleFournisseur = sh1.sh_cleFournisseur
JOIN 
    commandes o ON sh1.sh_cleCommande = o.o_cleCommande
WHERE 
    n.n_nom = 'ARGENTINA'
    AND o.o_statutCommande = 'F'
    AND sh1.sh_dateReception > sh1.sh_dateEngagement
    AND NOT EXISTS (
        SELECT 1
        FROM shineitem sh2
        WHERE sh1.sh_cleCommande = sh2.sh_cleCommande
          AND sh1.sh_cleFournisseur <> sh2.sh_cleFournisseur
          AND sh2.sh_dateReception > sh2.sh_dateEngagement
    )
GROUP BY 
    m.mnom
ORDER BY 
    num_wait DESC,
    s_name;


It's Output: 
"Supplier#000000748       "	30
"Supplier#000004227       "	29
"Supplier#000000985       "	27
"Supplier#000001110       "	26
"Supplier#000000730       "	25
"Supplier#000002320       "	25
"Supplier#000003512       "	25
"Supplier#000004550       "	25
"Supplier#000000521       "	24
"Supplier#000001902       "	24
"Supplier#000001823       "	23
"Supplier#000004385       "	23
"Supplier#000001270       "	22
"Supplier#000001402       "	22
"Supplier#000001573       "	22
"Supplier#000002057       "	22
"Supplier#000002122       "	22
"Supplier#000002686       "	22
"Supplier#000002843       "	22
"Supplier#000000849       "	21
"Supplier#000001136       "	21
"Supplier#000001224       "	21
"Supplier#000001647       "	21
"Supplier#000001771       "	21
"Supplier#000001810       "	21
"Supplier#000001991       "	21
"Supplier#000002612       "	21
"Supplier#000002734       "	21
"Supplier#000002745       "	21
"Supplier#000002883       "	21
"Supplier#000003321       "	21
"Supplier#000004004       "	21
"Supplier#000004198       "	21
"Supplier#000000485       "	20
"Supplier#000001084       "	20
"Supplier#000002547       "	20
"Supplier#000002808       "	20
"Supplier#000002886       "	20
"Supplier#000002957       "	20
"Supplier#000002977       "	20
"Supplier#000003174       "	20
"Supplier#000003311       "	20
"Supplier#000003404       "	20
"Supplier#000003426       "	20
"Supplier#000003510       "	20
"Supplier#000004301       "	20
"Supplier#000004503       "	20
"Supplier#000004581       "	20
"Supplier#000004798       "	20
"Supplier#000000003       "	19
"Supplier#000000945       "	19
"Supplier#000002250       "	19
"Supplier#000002429       "	19
"Supplier#000002928       "	19
"Supplier#000003006       "	19
"Supplier#000003210       "	19
"Supplier#000003581       "	19
"Supplier#000003791       "	19
"Supplier#000003813       "	19
"Supplier#000003916       "	19
"Supplier#000004050       "	19
"Supplier#000004252       "	19
"Supplier#000004309       "	19
"Supplier#000004803       "	19
"Supplier#000004841       "	19
"Supplier#000000678       "	18
"Supplier#000000721       "	18
"Supplier#000001076       "	18
"Supplier#000001811       "	18
"Supplier#000001884       "	18
"Supplier#000002982       "	18
"Supplier#000003789       "	18
"Supplier#000004080       "	18
"Supplier#000004213       "	18
"Supplier#000004344       "	18
"Supplier#000004434       "	18
"Supplier#000004573       "	18
"Supplier#000004593       "	18
"Supplier#000004640       "	18
"Supplier#000000297       "	17
"Supplier#000000567       "	17
"Supplier#000000714       "	17
"Supplier#000001186       "	17
"Supplier#000001360       "	17
"Supplier#000001957       "	17
"Supplier#000002017       "	17
"Supplier#000002174       "	17
"Supplier#000002463       "	17
"Supplier#000002821       "	17
"Supplier#000003065       "	17
"Supplier#000003351       "	17
"Supplier#000003859       "	17
"Supplier#000004210       "	17
"Supplier#000004466       "	17
"Supplier#000004627       "	17
"Supplier#000000950       "	16
"Supplier#000001743       "	16
"Supplier#000002107       "	16
"Supplier#000002111       "	16
"Supplier#000002128       "	16
"Supplier#000002199       "	16
"Supplier#000002291       "	16
"Supplier#000002332       "	16
"Supplier#000002502       "	16
"Supplier#000002733       "	16
"Supplier#000003209       "	16
"Supplier#000003483       "	16
"Supplier#000003787       "	16
"Supplier#000004350       "	16
"Supplier#000004605       "	16
"Supplier#000004756       "	16
"Supplier#000004762       "	16
"Supplier#000004909       "	16
"Supplier#000000071       "	15
"Supplier#000000127       "	15
"Supplier#000000336       "	15
"Supplier#000000544       "	15
"Supplier#000000624       "	15
"Supplier#000000852       "	15
"Supplier#000000868       "	15
"Supplier#000000873       "	15
"Supplier#000001544       "	15
"Supplier#000001854       "	15
"Supplier#000002143       "	15
"Supplier#000002359       "	15
"Supplier#000002519       "	15
"Supplier#000002899       "	15
"Supplier#000003453       "	15
"Supplier#000003495       "	15
"Supplier#000003559       "	15
"Supplier#000003570       "	15
"Supplier#000003636       "	15
"Supplier#000003743       "	15
"Supplier#000004248       "	15
"Supplier#000004359       "	15
"Supplier#000004407       "	15
"Supplier#000004457       "	15
"Supplier#000004777       "	15
"Supplier#000000186       "	14
"Supplier#000000539       "	14
"Supplier#000000836       "	14
"Supplier#000001533       "	14
"Supplier#000001936       "	14
"Supplier#000001965       "	14
"Supplier#000002052       "	14
"Supplier#000002633       "	14
"Supplier#000002803       "	14
"Supplier#000003005       "	14
"Supplier#000003010       "	14
"Supplier#000003601       "	14
"Supplier#000003804       "	14
"Supplier#000003841       "	14
"Supplier#000004277       "	14
"Supplier#000004282       "	14
"Supplier#000004745       "	14
"Supplier#000004892       "	14
"Supplier#000000430       "	13
"Supplier#000000886       "	13
"Supplier#000001280       "	13
"Supplier#000001734       "	13
"Supplier#000001918       "	13
"Supplier#000002491       "	13
"Supplier#000002880       "	13
"Supplier#000003119       "	13
"Supplier#000003535       "	13
"Supplier#000003816       "	13
"Supplier#000004053       "	13
"Supplier#000004584       "	13
"Supplier#000004646       "	13
"Supplier#000000029       "	12
"Supplier#000000518       "	12
"Supplier#000000792       "	12
"Supplier#000000989       "	12
"Supplier#000003194       "	12
"Supplier#000004214       "	12
"Supplier#000004773       "	12
"Supplier#000004863       "	12
"Supplier#000000184       "	11
"Supplier#000000244       "	11
"Supplier#000000725       "	11
"Supplier#000000801       "	11
"Supplier#000001020       "	11
"Supplier#000001124       "	11
"Supplier#000001213       "	11
"Supplier#000001596       "	11
"Supplier#000002058       "	11
"Supplier#000002762       "	11
"Supplier#000003554       "	11
"Supplier#000004881       "	11
"Supplier#000001509       "	10
"Supplier#000001660       "	10
"Supplier#000002493       "	10
"Supplier#000003661       "	10
"Supplier#000004189       "	10
"Supplier#000004834       "	10
"Supplier#000000203       "	9
"Supplier#000000230       "	9
"Supplier#000000363       "	9
"Supplier#000003790       "	9
"Supplier#000002316       "	8
"Supplier#000002515       "	8

Result is mismatching. Fix the query.

Final Query:
SELECT 
    m.mnom AS s_name,
    COUNT(*) AS num_wait
FROM 
    mopplier m
JOIN 
    nation n ON m.mcleNation = n.n_cleNation
JOIN 
    shineitem sh1 ON m.mcleFournisseur = sh1.sh_cleFournisseur
JOIN 
    commandes o ON sh1.sh_cleCommande = o.o_cleCommande
WHERE 
    n.n_nom = 'ARGENTINA'
    AND o.o_statutCommande = 'F'
    AND sh1.sh_dateReception > sh1.sh_dateEngagement
    AND NOT EXISTS (
        SELECT 1
        FROM shineitem sh2
        WHERE sh1.sh_cleCommande = sh2.sh_cleCommande
          AND sh1.sh_cleFournisseur <> sh2.sh_cleFournisseur
          AND sh2.sh_dateReception > sh2.sh_dateEngagement
    )
    AND EXISTS (
        SELECT 1
        FROM shineitem sh3
        WHERE sh1.sh_cleCommande = sh3.sh_cleCommande
          AND sh1.sh_cleFournisseur <> sh3.sh_cleFournisseur
    )
GROUP BY 
    m.mnom
ORDER BY 
    num_wait DESC,
    s_name;

"""


def one_round():
    print("----- streaming request -----")
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "user",
                "content": f"{text_2_sql_prompt}",
            },
        ], temperature=0, stream=True
    )
    for chunk in response:
        if not chunk.choices:
            continue

        print(chunk.choices[0].delta.content, end="")
        print("")


orig_out = sys.stdout
f = open('chatgpt_tpch_sql.py', 'w')
sys.stdout = f
one_round()
sys.stdout = orig_out
f.close()
