# Data 

This directory contains labeled data from:

- Ahmad, R. A., Borisova, E., & Rehm, G. (2024). FoRC4CL: A Fine-grained Field of Research Classification and Annotated Dataset of NLP Articles. In _Proceedings of the 2024 Joint International Conference on Computational Linguistics, Language Resources and Evaluation (LREC-COLING 2024)_ (pp. 7389-7394). https://aclanthology.org/2024.lrec-main.651.pdf
- Abu Ahmad, R., Borisova, E., & Rehm, G. (2024). FoRC4CL Corpus [Data set]. Zenodo. https://doi.org/10.5281/zenodo.10777674
- Abu Ahmad, R., Borisova, E., & Rehm, G. (2024). FoRC-Subtask-I@NSLP2024 Training and Validation Data [Data set]. Zenodo. https://doi.org/10.5281/zenodo.10438530
- Abu Ahmad, R., Borisova, E., & Rehm, G. (2024). FoRC-Subtask-I@NSLP2024 Testing Data [Data set]. Zenodo. https://doi.org/10.5281/zenodo.10469550
- Abu Ahmad, R., Borisova, E., & Rehm, G. (2024). FoRC-Subtask-II@NSLP2024 Training and Validation Data [Data set]. Zenodo. https://doi.org/10.5281/zenodo.10438581
- Abu Ahmad, R., Borisova, E., & Rehm, G. (2024). FoRC-Subtask-II@NSLP2024 Testing Data [Data set]. Zenodo. https://doi.org/10.5281/zenodo.10469576
- https://nfdi4ds.github.io/nslp2024/
- https://huggingface.co/datasets/katebor/taxonomy/blob/main/Taxonomy4CL_v1.0.1.json

We also have a [crosswalk](cset-forc-crosswalk.csv) of FoRC fields to CSET fields.

# Evaluation

1. Preprocess the FoRC data to match the format of our merged corpus data.
2. Run `scripts/score_corpus` over the FoRC data.
3. Use the [crosswalk](cset-forc-crosswalk.csv) of FoRC fields to CSET fields to evaluate our solution against the FoRC labels.