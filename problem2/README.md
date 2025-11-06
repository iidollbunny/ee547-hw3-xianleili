Name: Xianlei Li
Email: xianleil@usc.edu
Instructions:

python query_papers.py recent cs.LG --limit 5 --table arxiv-papers
python query_papers.py author "Hendrik Blockeel" --table arxiv-papers
python query_papers.py get 0110036v1 --table arxiv-papers
python query_papers.py daterange cs.LG 2000-01-01 2001-12-31 --table arxiv-papers
python query_papers.py keyword learning --limit 5 --table arxiv-papers

http://localhost:8081/papers/recent?category=cs.LG&limit=5
http://localhost:8081/papers/author/Hendrik%20Blockeel
http://localhost:8081/papers/0110036v1
http://localhost:8081/papers/search?category=cs.LG&start=2000-01-01&end=2001-12-31
http://localhost:8081/papers/keyword/learning?limit=5
