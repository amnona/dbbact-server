#!/bin/bash
source activate dbbact
nohup dbbact_jobs/run_bg_jobs.py --debug-level 2 --server-type main -p update_seq_translator:wholeseq-file:~/whole_seqs/SILVA_132_SSURef_tax_silva.fasta -p update_seq_translator:user:sequence_translator_dbbact -p update_seq_translator:password:sequence_translator_dbbact -p update_seq_translator:database:sequence_translator_dbbact -p update_seq_taxonomy:rdp:~/rdp_classifier_2.12/ > ~/logs/main-bg-scripts-output.txt &
echo "started main run_bg_jobs.py"
