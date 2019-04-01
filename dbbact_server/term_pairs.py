from collections import defaultdict
import requests
from .Site_Main_Flask import get_db_address


def get_term_pairs_score(annotations, min_exp=2, get_pairs=True, get_singles=True):
	'''Get the term-pairs (i.e. homo spaiens+feces) score based on the annotations

	Parameters
	----------
	annotations: list of dict
		list of the dbbact annotations
	min_exp: int, optional
		the minimal number of experiments for the term-pair to appear in order to use it

	Returns
	-------
	dict of {str: float}
		key is the term-pair string ("homo sapiens+feces")
		value is the score (sum over all experiments of the fraction of annotations (in the experiment) containing this term pair where it appears)
	'''
	# group the annotations by experiment (a dict of annotationid:annotation for each experiment)
	experiments = set()
	for cann in annotations:
		experiments.add(cann['expid'])

	# get all the experiment annotations and count the number of appearances of each term pair in each annotation
	exp_term_pairs = {}
	term_pair_exps = defaultdict(set)
	for cexp in experiments:
		cexp_term_pairs = defaultdict(float)
		res = requests.get(get_db_address() + '/experiments/get_annotations', json={'expId': cexp})
		cannotations = res.json()['annotations']
		for ccann in cannotations:
			cterm_pairs = get_annotation_term_pairs(ccann, get_pairs=get_pairs, get_singles=get_singles)
			for ccterm_pair in cterm_pairs:
				# add one count to the number of annotations in the experiment where the term appears
				cexp_term_pairs[ccterm_pair] += 1
				# and add this experiment to the experiment list for the term pair
				term_pair_exps[ccterm_pair].add(cexp)
		exp_term_pairs[cexp] = cexp_term_pairs

	term_pair_score = defaultdict(float)
	for cann in annotations:
		cexp = cann['expid']
		cterm_pairs = get_annotation_term_pairs(cann)
		for ccterm_pair in cterm_pairs:
			if len(term_pair_exps[ccterm_pair]) >= min_exp:
				term_pair_score[ccterm_pair] += 1 / exp_term_pairs[cexp][ccterm_pair]

	return term_pair_score


def get_annotation_term_pairs(cann, max_terms=20, get_pairs=True, get_singles=True):
	'''Get the pairs of terms in the annotation and their type

	Parameters
	----------
	cann : dict
		items of the output of get_seq_annotations()

	Returns
	-------
	list of str of term1 + "+" + term2 (sorted alphabetically term1<term2)
	if term is "lower in", it will be preceeded by "-"
	'''
	term_pairs = []
	details = cann['details']
	if len(details) <= max_terms:
		for p1 in range(len(details)):
			# print('now detail term idx %d' % p1)
			for p2 in range(p1 + 1, len(details)):
				det1 = details[p1]
				det2 = details[p2]
				term1 = det1[1]
				term2 = det2[1]
				type1 = det1[0]
				type2 = det2[0]
				if type1 == 'low':
					term1 = '-' + term1
				if type2 == 'low':
					term2 = '-' + term2
				cnew_type = 'all'
				if type1 == type2:
					cnew_type == type1
				cnew_term = sorted([term1, term2])
				cnew_term = "+".join(cnew_term)
				# cnew_term = '%s+%s' % (term1, term2)
				term_pairs.append(cnew_term)
		# print('new details: %d' % len(details))
	return term_pairs
