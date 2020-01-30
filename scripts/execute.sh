#!/bin/bash

python3 elements_per_protocol.py
python3 pages_per_protocol.py
python3 pages_with_sri.py
python3 require_sri_for.py
python3 sri_per_algorithm.py
python3 sri_per_host_and_target_protocol.py
python3 sri_per_page_evolution.py
python3 sri_per_page.py
python3 sri_per_protocol.py
python3 statistics.py
python3 top_resources.py
python3 top_sri_domain.py
python3 top_sri_evolution.py
python3 survey.py

rm -f output/*-crop.pdf; for i in output/*.pdf; do pdfcrop $i; done