#!/usr/bin/env python3
"""
inventory_case_study.py

Data Analysis Case Study - Online Marketplace Inventory

Usage:
    python3 inventory_case_study.py \
        --input data/inventory.csv \
        --output report.txt

This script will:
 1. Load inventory data (epoch-timestamp columns + product metadata)
 2. Clean vendor padding, skip missing reports, filter out implausible outliers
 3. Deduplicate products whose inventory series are identical
 4. Identify top 20 performing products by quantity sold
 5. Answer:
    Q1. Unpacking Excellence (quantity sold & avg stock)
    Q2. Impact of Strategies & Events (placeholder)
    Q3. Recommendations for Improvement (placeholder)
 6. Save a plain-text report to --output
"""

import argparse
from datetime import datetime

import pandas as pd
import numpy as np

MAX_REAL_STOCK = 100_000  # any reading above this is treated as missing

# Helper: strip vendor padding, preserve NaN
def strip_padding(val):
    if pd.isna(val):
        return np.nan
    try:
        v = int(float(val))
    except (ValueError, TypeError):
        return np.nan
    s = str(v)
    head, tail = s[:-4], s[-4:]
    if not head:
        return v
    if set(head) == {"9"} or (head[0] in {"1","2"} and set(head[1:]) == {"9"}):
        return int(tail.lstrip("0") or 0)
    return v

def load_data(path):
    df = pd.read_csv(path)
    # drop stray index columns
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]

    id_cols   = ["Product", "Category", "BrandId"]
    time_cols = [c for c in df.columns if c not in id_cols]

    df_long = df.melt(
        id_vars=id_cols,
        value_vars=time_cols,
        var_name="epoch",
        value_name="inventory"
    )

    # clean inventory: strip padding, filter outliers, skip blanks
    df_long["inventory"] = (
        pd.to_numeric(df_long["inventory"], errors="coerce")
          .apply(strip_padding)
          .where(lambda s: s <= MAX_REAL_STOCK, np.nan)
    )
    # drop rows with no report
    df_long = df_long.dropna(subset=["inventory"])
    df_long["inventory"] = df_long["inventory"].astype(int)

    # convert epoch to timestamp
    df_long["timestamp"] = pd.to_datetime(df_long["epoch"].astype(int), unit="s")

    return df_long.drop(columns=["epoch"]).sort_values(["Product","timestamp"])

def dedupe_products(df_long):
    # pivot to get series per product
    pivot = df_long.pivot_table(
        index="Product", columns="timestamp", values="inventory", fill_value=np.nan
    )
    # fingerprint each series
    fps = pivot.apply(lambda row: hash(tuple(row)), axis=1)
    # keep first occurrence of each fingerprint
    unique = fps[~fps.duplicated()].index
    # filter df_long
    return df_long[df_long["Product"].isin(unique)]

def quantity_sold(x):
    diffs = x.diff()
    return diffs.where(diffs < 0).abs().sum()


def identify_top_products(df_long, top_n=20):
    sold = (
        df_long.groupby("Product")["inventory"]
               .apply(quantity_sold)
               .sort_values(ascending=False)
    )
    return sold[sold > 0].head(top_n).index.tolist()


def analyze_unpacked_excellence(df_long, top_products):
    sales = (
        df_long[df_long["Product"].isin(top_products)]
               .groupby("Product")["inventory"]
               .apply(quantity_sold)
    )
    avg_stock = (
        df_long[df_long["Product"].isin(top_products)]
               .groupby("Product")["inventory"]
               .mean()
    )
    summary = pd.DataFrame({
        "quantity_sold": sales,
        "avg_stock": avg_stock
    }).sort_values("quantity_sold", ascending=False)

    report = "Top 20 Performers by Quantity Sold & Average Stock:\n"
    report += summary.to_string(formatters={
        "quantity_sold": "{:,.0f}".format,
        "avg_stock": "{:,.0f}".format
    })
    return report


def analyze_strategies_events(df_long, top_products):
    return (
        "Impact analysis placeholder.\n"
        "(Join with event calendar or marketing logs on date.)"
    )

def identify_underperformers(df_long, bottom_pct=10):
    sold = df_long.groupby("Product")["inventory"].apply(quantity_sold)
    cutoff = np.percentile(sold[sold > 0], bottom_pct)
    return sold[sold <= cutoff].index.tolist()

def recommendations(_df_long, _underperformers):
    return (
        "Recommendations placeholder:\n"
        "- Bundle underperformers with best-sellers\n"
        "- Test targeted discounts or refreshed imagery\n"
        "- Review pricing for low-movement SKUs"
    )

# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
def main(input_csv, output_report):
    df_long = load_data(input_csv)
    df_long = dedupe_products(df_long)             # remove identical-series SKUs

    top20   = identify_top_products(df_long)
    bottom10= identify_underperformers(df_long)

    q1 = analyze_unpacked_excellence(df_long, top20)
    q2 = analyze_strategies_events(df_long, top20)
    q3 = recommendations(df_long, bottom10)

    with open(output_report, "w") as f:
        f.write("Data Analysis Case Study Report\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n\n")
        f.write("=== Q1: Unpacking Excellence ===\n"    + q1 + "\n\n")
        f.write("=== Q2: Strategies & Events Impact ===\n" + q2 + "\n\n")
        f.write("=== Q3: Recommendations for Underperformers ===\n" + q3 + "\n")

    print(f"✔ Report written to {output_report}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inventory Turnover Analyzer")
    parser.add_argument("--input",  "-i", required=True, help="Path to inventory CSV")
    parser.add_argument("--output", "-o", required=True, help="Output report file")
    args = parser.parse_args()
    main(args.input, args.output)







'''Data Analysis Case Study Report
Generated: 2025-05-01T12:53:26.299278

=== Q1: Unpacking Excellence ===
Top 20 Performers by Quantity Sold & Average Stock:
                                            quantity_sold avg_stock
Product                                                            
Brewer's Cookies  - Chocolate Chip/Sea Salt        88,383    71,483
Dark Chocolate Grain Free Pretzels                 56,338    14,137
Cranberry Almond Crisps                            56,151    10,113
Earl Grey Shortbread                               41,840     3,224
Freeze Dried Crunchy Bears                         39,651    38,566
Gummy Sour Mix (16 oz)                             38,700     4,570
Gummy Peach Rings (14 oz)                          29,100     3,327
Gummy Rainforest Frogs (15 oz)                     28,866     6,064
Gummy Sour Worms (14 oz)                           28,699     6,150
Cactus Rose Sparkling Water                        27,618    13,482
Ancho Villa Biltong 2oz                            27,386    19,447
Wax Bottles                                        27,293     4,838
Moose bar                                          27,077     5,245
Dark Chocolate Coated Almonds                      21,375     8,999
Classic 100% Grass-Fed Beef Jerky, 1.0oz           21,072     4,873
Fruit and Nut Bars - Cashew & Cranberry            20,211     5,857
Pretzel Shortbread Cookies                         19,924     2,028
GUMMY FROGS                                        19,743    17,461
Black Ops Chocolate Bar                            19,584     3,285
Orange Cream Truffle                               19,552     3,291

Almost all of the top 20 highest performing products can be classified as snacks or treats.
This includes items such as cookies, crisps, gummies, jerky, chocolate bars, etc. These product
typically do well since they are inherently implulse buys. In fact, Euromonitor senior analyst Jared
Koerten noted during the Sweet & Snack Expo that, "nearly 70% of in-store purchases are unplanned
- indicating that impulse snack shopping is a shared habit among most consumers." People generally buy 
food based on impulse decisions. The results extracted from this dataset prove that the majority of these
impulse buys fall under the snack/treat catagory which are generally more unhealthy than other alternatives.
As a BMC Public Health study explains, "impulse food purchases (unplanned) have been shown to often be both
unhealthy and heavily influenced by the presence of within-store displays and promotions." These findings suggests
that top-performing SKUs aren't just well liked flavors, they're also the ones most likely to catch a shopper's eye
in a fleeting decision moment. This drives higher turnover rates in less healthful products.

Although the majority of these results are unhealthy products, there is a subset of items that dont fall under that 
category. For example, products like "Cactus Rose Sparkling Water" and "Ancho Villa Biltong" are examples of 
unique flavoring combinations and gormet options which make the products seem more premium and stand out. Another 
sub category that is relevant among the top 20 highest performing products are healthy products. This includes items
such as "Dark Chocolate Coated Almonds" and "Classic 100% Grass-Fed Beef Jerky" which attract a different type of demographic
that prioritizes health over other aspects. These two subsets are noteable however it is clear that unhealthy snacks and treats
still remain as the dominant overarching category. 



=== Q2: Strategies & Events Impact ===
Event/strategy analysis:

Brewers Cookies:
Brewer's Foods advertizes their cookies as being sustainable and upcycled. By doing this, they are essentially finding a 
crossroads between healthy eaters and people who enjoy junk food. In January 2023 they rolled out their choclate chip
sea salt cookie made with upcycled brewer's grain, Callebaut chocolate, Cabot butter, and King Arthur flour. All of these 
ingredients are ethically and sustainably sourced which earned featured coverage in the Food Engineering magazine and helped
differentiate the SKU on the shelf and online.

Dark Chocolate Grain-Free Pretzels:
The Hershey Company's Flipz line (chocolate covered pretzels) grew 11.3% in a year by essentially combining retailer led promotions
and product innovation. They rolled out new limited edition flavors, ran end cap displays and in store coupons, and then also doubled 
down on e-commerce partnerships. This ended up resulting in a $66.6 M in annual Flipz sales and lifting the broader chocolate pretzel
segment.

Canberry Almond Crisps:
Main Crisp Co. builds demand for their products by positioning these crisps for cheeseboard pairings and featuring user testimonials
prominantly on their site. Their "recipe spotlight" content creates social medai attention, while at the same time, Amazon's "frequently 
bought together" widget cross-sells the crisps alongside complementary items. 


=== Q3: Recommendations for Underperformers ===
Recommendations placeholder:
1) Bundle slow-movers with best sellers:
    Create a "dual snack" or “taste-tester” bundles that pair one underperforming SKU (say, a niche jam flavor or specialty olive oil) 
    with a guaranteed crowd pleaser from your top 20. Price the bundle at a 5 - 10 % discount versus buying separately and feature it in 
    a dedicated on-site collection (“Mix & Match Snack Duo”) and in your next email newsletter. This not only piggybacks the weaker product's 
    visibility on the strength of a proven seller but also gives customers a low risk way to try something new.
2)Attempt to find intersections with different demographics:
    A perfect example of this is with Brewer's cookies and how they switch to eco freindly ingredients in their products. This strategy links
    two distinct demographics together; people who like healthy and sustainable food, and people who like sweets. By extending a products demographic,
    it will be more appealing to more potential customers. 
3)In-store positioning
    The majority of the top performing items could be classified as "impuled buys". A large part of impulse buys has to do with the products 
    positioning within the store. For example, items that are placed right at the checkout line are more likely to be bought impulsively than 
    products that are in the very back of the store. 
'''