import pandas as pd
import os
from pathlib import Path

file_path = Path(os.path.realpath(__file__))

# csv source: https://github.com/maltfield/awesome-lemmy-instances
df = pd.read_csv(
    file_path.parent.parent.joinpath("data").joinpath("awesome-lemmy-instances.csv")
)

instances = df["Instance"].tolist()

# hacky but works
urls = [instance.split("](")[1][:-1] for instance in instances]
sites = [url.split("//")[1].split("/")[0] for url in urls]


def rule(site):
    s = """Rule {{
    Matches {{
        Site("|{0}|")
    }}
}};""".format(
        site
    )

    return s


optic = """DiscardNonMatching;

Rule {
    Matches {
        Schema("QAPage"),
    }
};

Rule {
    Matches {
        Schema("DiscussionForumPosting"),
    }
};

Rule {
    Matches {
        Domain("reddit.com"),
        Url("comments"),
    }
};

"""

optic += (
    "// source of instances: https://github.com/maltfield/awesome-lemmy-instances\n"
)
optic += "\n\n".join([rule(site) for site in sites])

print(optic)
