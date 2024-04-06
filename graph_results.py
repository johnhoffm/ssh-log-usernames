import matplotlib.pyplot as plt
import pandas as pd

df = pd.read_csv('usernames_count.csv', delimiter=',')
total = df["count"].sum()
top_50_df = df.head(50)

fig, ax = plt.subplots()
plt.barh(top_50_df["usernames"], top_50_df["count"] / total * 100)
ax.invert_yaxis()
plt.title("Top 50 usernames for attempting to connect on ssh")
plt.ylabel("Username")
plt.xlabel("Percentage of times attempted")

# font lower and size higher to fit all usernames
fig.set_figheight(8)
plt.yticks(fontsize=8)
plt.savefig("top_50_usernames.png", bbox_inches='tight')
