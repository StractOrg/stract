import lightgbm as lgb
import matplotlib.pyplot as plt
import sys

model = lgb.Booster(model_file=sys.argv[1])

img = lgb.plot_tree(model, tree_index=0, figsize=(20, 8), show_info=[
    "leaf_value", "feature_name", "threshold"])
plt.show()
