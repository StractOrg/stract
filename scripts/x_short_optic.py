import argparse

parser = argparse.ArgumentParser()
parser.add_argument('million_short_file', type=str)
parser.add_argument('-n', type=int, default=10_000)

args = parser.parse_args()

domains = []
with open(args.million_short_file, 'r') as f:
    for n, line in enumerate(f):
        if n >= args.n:
            break

        domain = line.strip().split(',')[1]
        domains.append(domain)

optic = "// Generated from the following list: https://tranco-list.eu/ \n"
for domain in domains:
    optic += f"""Rule {{ Matches {{ Domain("|{domain}|") }}, Action(Discard) }};"""

print(optic)
