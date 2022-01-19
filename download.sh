out="/home/dx789"
tmp="/home/dx789/tmp"
data="/home/dx789/AudioSet/eval_segments.csv"
ontology="/home/dx789/AudioSet/ontology.json"
num_workers=2
num_postprocessors=1
user="dx789"
domain="cs.utexas.edu"
hostnames="/home/dx789/AudioSet/cs_hosts.txt"
exp_dir="/home/dx789"

python3 yt-dl.py --out $out --tmp $tmp --data $data --ontology $ontology --num-workers $num_workers --num-postprocessors $num_postprocessors --user $user --domain $domain --hostnames $hostnames --exp-dir $exp_dir
