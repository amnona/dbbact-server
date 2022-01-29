#!/usr/bin/env python

''' Update the AnnotationParentsTable and the corresponding sequence counts
use it after adding/updating/changing an ontology in dbbbact
'''

import sys
import argparse
import setproctitle
from collections import defaultdict

import psycopg2

from dbbact_server import db_access, dbannotations, dbontology
from dbbact_server.utils import debug, SetDebugLevel

__version__ = "1.0"


def update_ontology_parents_overwrite(con, cur):
    skipped = 0
    added = 0
    all_parents_dict = {}
    seq_counts = defaultdict(int)
    annotation_counts = defaultdict(int)
    annotation_neg_counts = defaultdict(int)
    exp_set = defaultdict(set)

    debug(4, 'updating parents for all terms/ontologies')
    debug(4, 'deleting all from annotationparentstable')
    cur.execute('DELETE FROM AnnotationParentsTable')
    debug(4, 'temporarily deleting indices on AnnotationParentsTable')
    try:
        cur.execute('DROP INDEX annotationparentstable_idannotation_idx')
        cur.execute('DROP INDEX annotationparentstable_ontology_idx')
    except:
        con.rollback()
        debug(7, 'failed to delete indices from AnnotationParentsTable')

    # iterate over all annotations
    cur.execute('SELECT id,seqCount,idexp from AnnotationsTable')
    annotations = cur.fetchall()
    debug(4, 'updating AnnotationParentsTable for %d annotations' % len(annotations))
    for cres in annotations:
        cid = cres['id']
        cseqcount = cres['seqcount']
        cexp = cres['idexp']
        if cseqcount == 0:
            debug(5, 'WARNING: annotation %d has no sequences in AnnotationsTable' % cid)

        err, annotationdetails = dbannotations.get_annotation_details_termids(con, cur, cid)
        if err:
            debug(6, 'error: %s' % err)
            continue
        debug(3, 'adding annotation %d (%d)' % (cid, added))

        # add the annotation parents
        numadded = 0
        parentsdict = {}
        for (cdetailtype, contologyterm) in annotationdetails:
            contologyterm = contologyterm.lower()
            parents = None
            if contologyterm in all_parents_dict:
                parents = all_parents_dict[contologyterm]

            # if we don't yet have the parents, get from table
            if parents is None:
                err, parents = dbontology.get_parents(con, cur, contologyterm)
                if err:
                    debug(6, 'error getting parents for term %s: %s' % (contologyterm, err))
                    all_parents_dict[contologyterm] = []
                    continue
                all_parents_dict[contologyterm] = parents

            debug(2, 'term %s parents %s' % (contologyterm, parents))
            if cdetailtype not in parentsdict:
                parentsdict[cdetailtype] = parents.copy()
            else:
                parentsdict[cdetailtype].extend(parents)

        for cdetailtype, parents in parentsdict.items():
            parents = list(set(parents))
            for cpar in parents:
                err, cparent_description, cparent_term_id = dbontology.get_name_from_id(con, cur, cpar)
                if err:
                    debug(7, err)
                    continue
                cdetailtype = cdetailtype.lower()
                debug(1, 'adding parent %s' % cpar)
                cur.execute('INSERT INTO AnnotationParentsTable (idAnnotation,annotationDetail,ontology, term_id) VALUES (%s,%s,%s,%s)', [cid, cdetailtype, cparent_description, cparent_term_id])
                numadded += 1
                # add the number of sequences and one more annotation to all the terms in this annotation (we need to update the ontology table later)
                if cdetailtype == 'all' or cdetailtype == 'high':
                    annotation_counts[cparent_term_id] += 1
                elif cdetailtype == 'low':
                    annotation_neg_counts[cparent_term_id] += 1
                else:
                    debug(6, 'cdetailtype %s not recognized for annotation %s' % (cdetailtype, cid))
                exp_set[cparent_term_id].add(cexp)
                seq_counts[cparent_term_id] += cseqcount

        debug(1, "Added %d annotationparents items" % numadded)
        added += 1

    debug(4, 're-adding indexes to annotationparentstable')
    cur.execute('CREATE INDEX annotationparentstable_idannotation_idx ON annotationparentstable(idannotation int4_ops)')
    cur.execute('CREATE INDEX annotationparentstable_ontology_idx ON annotationparentstable(ontology text_ops)')

    debug(4, 'updating the ontologytable (tmp) sequence and annotation counts for each term')
    # we need to copy and insert new values since updating inplace is very very slow
    # create the tmp copy but without indexes
    cur.execute('CREATE TABLE tmp_ontologytable (LIKE ontologytable INCLUDING defaults INCLUDING constraints)')
    insert_cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute('SELECT * FROM OntologyTable')
    for row in cur:
        cterm = row['term_id']
        row['seqcount'] = seq_counts[cterm]
        row['annotationcount'] = annotation_counts[cterm]
        row['annotation_neg_count'] = annotation_neg_counts[cterm]
        row['exp_count'] = len(exp_set[cterm])

        cols = list(row.keys())
        vals = [row[x] for x in cols]

        insert_s = ['%s'] * len(cols)
        insert_s = '(' + ','.join(insert_s) + ')'
        insert_v = cols.copy()
        insert_v.extend(vals)
        insert_p1 = 'INSERT INTO tmp_ontologytable (%s) VALUES ' % ','.join(cols)
        insert_cur.execute(insert_p1 + insert_s, vals)

    debug(4, 'removing foreign key constraints')
    cur.execute('ALTER TABLE annotationlisttable DROP CONSTRAINT annotationlisttable_idontology_fkey')
    cur.execute('ALTER TABLE ontologysynonymtable DROP CONSTRAINT ontologysynonymtable_idontology_fkey')
    cur.execute('ALTER TABLE ontologytreestructuretable DROP CONSTRAINT ontologytreestructuretable_ontologyid_fkey')
    cur.execute('ALTER TABLE ontologytreestructuretable DROP CONSTRAINT ontologytreestructuretable_ontologyparentid_fkey')

    debug(4, 'deleting old ontologytable')
    cur.execute('DROP TABLE ontologytable CASCADE')

    debug(4, 'renaming new tmp_ontologytable to ontologytable')
    cur.execute('ALTER TABLE tmp_ontologytable RENAME TO ontologytable')

    # also create the sequence for automatic id
    debug(4, 'creating the ontologytable id sequence and linking')
    try:
        cur.execute('CREATE SEQUENCE ontologytable_id_seq')
    except:
        debug(2, 'the sequence ontologytable_id_seq already exists')
    # note: we skip the sanitization since this is run locally!!!
    cur.execute('ALTER SEQUENCE ontologytable_id_seq owner to %s' % con.get_dsn_parameters()['user'])
    cur.execute("SELECT setval('ontologytable_id_seq', (SELECT max(id)+1 FROM ontologytable), false)")
    cur.execute("ALTER TABLE ontologytable ALTER COLUMN id SET DEFAULT nextval('ontologytable_id_seq')")
    # we need to set the owner of the sequence to the same owner as the database - to enable updating it
    cur.execute('ALTER SEQUENCE ontologytable_id_seq owned by ontologytable.id')

    debug(4, 'creating indices for ontologytable')
    cur.execute('CREATE UNIQUE INDEX ontologytable_pkey ON ontologytable(id int4_ops)')
    cur.execute('CREATE INDEX ontologytable_description_index ON ontologytable(description text_ops)')
    cur.execute('CREATE INDEX term_id_index ON ontologytable(term_id text_ops)')

    debug(4, 're-linking foreign keys to ontologytable')
    cur.execute('ALTER TABLE annotationlisttable ADD CONSTRAINT annotationlisttable_idontology_fkey FOREIGN KEY (idontology) REFERENCES ontologytable(id)')
    cur.execute('ALTER TABLE ontologysynonymtable ADD CONSTRAINT ontologysynonymtable_idontology_fkey FOREIGN KEY (idontology) REFERENCES ontologytable(id)')
    cur.execute('ALTER TABLE ontologytreestructuretable ADD CONSTRAINT ontologytreestructuretable_ontologyid_fkey FOREIGN KEY (ontologyid) REFERENCES ontologytable(id)')
    cur.execute('ALTER TABLE ontologytreestructuretable ADD CONSTRAINT ontologytreestructuretable_ontologyparentid_fkey FOREIGN KEY (ontologyparentid) REFERENCES ontologytable(id)')

    debug(4, 'committing')
    con.commit()
    debug(4, 'added %d, skipped %d' % (added, skipped))


def update_ontology_parents(con, cur, overwrite=True, ontology=None):
    '''
    Fill the database AnnotationParentsTable

    Parameters
    ----------
    con, cur: dbBact psycopg2 database connection and cursor
    overwrite : bool (optional)
        False (default) to not overwrite existing annotation parents, True to delete all
    ontology: str or None (optional)
        if None, update all ontologies. Otherwise, update only terms from the given ontology
    '''
    debug(4, 'updating ontology parents')
    if ontology is not None:
        raise ValueError('ontology specific operation not supported yet!')
    skipped = 0
    added = 0
    all_parents_dict = {}
    if overwrite:
        if ontology is None:
            debug(4, 'deleting old parent counts')
            # delete the current counts since we are updating all entries (and addparents adds 1 to the counts...)
            # another way to do it (faster in postgres 11)?
            # alter table ontologytable drop column seqcount;
            # alter table ontologytable drop column annotationcount;
            # alter table ontologytable ADD COLUMN seqCount integer DEFAULT 0;
            # alter table ontologytable ADD COLUMN annotationCount integer DEFAULT 0;

            cur.execute('ALTER TABLE OntologyTable DROP COLUMN seqCount, DROP COLUMN annotationCount, DROP COLUMN annotation_neg_count')
            debug(4, 'inserting the columns')
            cur.execute('ALTER TABLE OntologyTable ADD COLUMN seqcount integer DEFAULT 0, ADD COLUMN annotationcount integer DEFAULT 0, ADD COLUMN annotation_neg_count integer DEFAULT 0')
            # cur.execute('UPDATE OntologyTable SET seqCount=0, annotationCount=0')
            debug(4, 'deleting annotationparentstable')
            cur.execute('DELETE FROM AnnotationParentsTable')
            debug(4, 'temporarily deleting indices on AnnotationParentsTable')
            cur.execute('DROP INDEX annotationparentstable_idannotation_idx')
            cur.execute('DROP INDEX annotationparentstable_ontology_idx')

    # iterate over all annotations
    cur.execute('SELECT id,seqCount from AnnotationsTable')
    annotations = cur.fetchall()
    debug(4, 'updating AnnotationParentsTable for %d annotations' % len(annotations))
    for cres in annotations:
        cid = cres[0]
        # if cid != 2305:
        #   continue
        cseqcount = cres[1]
        if cseqcount == 0:
            debug(5, 'WARNING: annotation %d has no sequences in AnnotationsTable' % cid)

        # if not in overwrite mode, don't add parents to entries already in the table
        if not overwrite:
            cur.execute('SELECT idAnnotation from AnnotationParentsTable WHERE idAnnotation=%s', [cid])
            if cur.rowcount > 0:
                skipped += 1
                continue

        err, annotationdetails = dbannotations.get_annotation_details_termids(con, cur, cid)
        if err:
            debug(6, 'error: %s' % err)
            continue
        debug(3, 'adding annotation %d (%d)' % (cid, added))
        dbannotations.AddAnnotationParents(con, cur, cid, annotationdetails, numseqs=cseqcount, all_parents_dict=all_parents_dict, commit=False)
        added += 1

    if overwrite:
        debug(4, 'adding indexes')
        cur.execute('CREATE INDEX annotationparentstable_idannotation_idx ON annotationparentstable(idannotation int4_ops)')
        cur.execute('CREATE INDEX annotationparentstable_ontology_idx ON annotationparentstable(ontology text_ops)')
    debug(4, 'committing')
    con.commit()
    debug(4, 'added %d, skipped %d' % (added, skipped))


def main(argv):
    parser = argparse.ArgumentParser(description='Update the AnnotationParentsTable after changing ontologies. version ' + __version__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--port', help='postgres port', default=5432, type=int)
    parser.add_argument('--host', help='postgres host', default=None)
    parser.add_argument('--database', help='postgres database', default='dbbact')
    parser.add_argument('--user', help='postgres user', default='dbbact')
    parser.add_argument('--password', help='postgres password', default='magNiv')
    parser.add_argument('--proc-title', help='name of the process (to view in ps aux)')
    parser.add_argument('--debug-level', help='debug level (1 for debug ... 9 for critical)', default=2, type=int)
    parser.add_argument('--ontology', help='ontology to update parents for (empty to update all')
    parser.add_argument('--redo-all', help='if set, recalculate for all ontologies/terms (slower)', action='store_true')
    args = parser.parse_args(argv)

    SetDebugLevel(args.debug_level)
    # set the process name for ps aux
    if args.proc_title:
        setproctitle.setproctitle(args.proc_title)

    con, cur = db_access.connect_db(database=args.database, user=args.user, password=args.password, port=args.port, host=args.host)
    if args.redo_all:
        update_ontology_parents_overwrite(con, cur)
    else:
        update_ontology_parents(con, cur, ontology=args.ontology)


if __name__ == "__main__":
    main(sys.argv[1:])
