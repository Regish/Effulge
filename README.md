# Effulge

## Use Case
- When we have two pyspark dataframes with valid Primary Key
- and we need to find the attributes that are mismatching between the two dataframes.
-----
## Example
Lets consider two dataframes "**expectation**" and "**reality**".
### *Inputs*
If Primary Key is (*ProductID, Colour*)
and if the contents of "**expectation**" are -
<table border=solid>
  <tr><th style="background-color:lightgreen">ProductID</th><th>ProductName</th><th style="background-color:lightgreen">Colour</th><th>UnitPrice</th><th>Quantity</th><th>Fragile</th><th>Gift</th></tr>
  <tr><td>1001</td><td>GelPen</td><td>Blue</td><td>10</td><td>2</td><td>0</td><td>0</td></tr>
  <tr><td>1001</td><td>GelPen</td><td>Black</td><td>10</td><td>1</td><td>0</td><td>0</td></tr>
  <tr><td>1001</td><td>GelPen</td><td>Red</td><td>10</td><td>1</td><td>0</td><td>0</td></tr>
  <tr><td>1002</td><td>InkPen</td><td>Blue</td><td>50</td><td>1</td><td>0</td><td>1</td></tr>
  <tr><td>1003</td><td>InkBottle</td><td>Blue</td><td>35</td><td>1</td><td>1</td><td>1</td></tr>
  <tr><td>1004</td><td>Pencil</td><td>Grey</td><td>3</td><td>5</td><td>0</td><td>0</td></tr>
  <tr><td>1005</td><td>Eraser</td><td>White</td><td>2</td><td>2</td><td>0</td><td>0</td></tr>
  <tr><td>1006</td><td>Sharpner</td><td>Orange</td><td>3</td><td>1</td><td>0</td><td>0</td></tr>
  <tr><td>1006</td><td>Sharpner</td><td>Steel</td><td>5</td><td>1</td><td>0</td><td>0</td></tr>
  <tr><td>1007</td><td>GeometryBox</td><td>Green</td><td>40</td><td>0</td><td>0</td><td></td></tr>
</table>

And if contents of "**reality**" are -
<table border=solid>
  <tr><th style="background-color:lightgreen">ProductID</th><th>ProductName</th><th style="background-color:lightgreen">Colour</th><th>UnitPrice</th><th>Quantity</th><th>Fragile</th><th>Gift</th></tr>
  <tr><td>1001</td><td>GelPen</td><td>Blue</td><td>10</td><td>2</td><td>0</td><td>0</td></tr>
  <tr><td>1001</td><td>GelPen</td><td>Black</td><td>10</td><td style="background-color:yellow">7</td><td>0</td><td>0</td></tr>
  <tr><td>1001</td><td>GelPen</td><td>Red</td><td>10</td><td>1</td><td>0</td><td>0</td></tr>
  <tr><td>1002</td><td>InkPen</td><td>Blue</td><td>50</td><td>1</td><td>0</td><td>1</td></tr>
  <tr><td>1003</td><td>InkBottle</td><td>Blue</td><td style="background-color:yellow">3</td><td>1</td><td>1</td><td style="background-color:yellow">0</td></tr>
  <tr style="background-color:orange"><td>1003</td><td>WaterBottle</td><td>Blue</td><td>20</td><td>2</td><td>0</td><td>0</td></tr>
  <tr><td>1004</td><td>Pencil</td><td>Grey</td><td>3</td><td>5</td><td>0</td><td>0</td></tr>
  <tr><td>1005</td><td>Eraser</td><td style="background-color:yellow">Whiteee</td><td>2</td><td>2</td><td>0</td><td>0</td></tr>
  <tr><td>1006</td><td>Sharpner</td><td>Orange</td><td>3</td><td>1</td><td>0</td><td>0</td></tr>
  <tr><td>1006</td><td>Sharpner</td><td>Steel</td><td>5</td><td>1</td><td>0</td><td>0</td></tr>
  <tr><td>1007</td><td>GeometryBox</td><td>Green</td><td>40</td><td>0</td><td style="background-color:yellow">1</td><td></td></tr>
</table>

### *Output*
Then, Effulge will produce an output dataframe with following contents -
<table border=solid>
  <tr><th style="background-color:lightgreen">productid</th><th style="background-color:lightgreen">colour</th><th>EFFULGE_VARIANCE_PROVOKER</th></tr>
  <tr><td>1007</td><td>Green</td><td>[fragile]</td></tr>
  <tr><td>1001</td><td>Black</td><td>[quantity]</td></tr>
  <tr><td>1003</td><td>Blue</td><td>[fragile, gift, productname, quantity, unitprice]</td></tr>
  <tr><td>1003</td><td>Blue</td><td>[gift, unitprice]</td></tr>
  <tr><td>1005</td><td>White</td><td>[MISSING_PRIMARY_KEY]</td></tr>
  <tr><td>1003</td><td>Blue</td><td>[DUPLICATE_PRIMARY_KEY]</td></tr>
</table>

-----

## Usage:

```python
from effulge import spot_variance

# Initialize SparkSession
# Load data into dataframes, let's say they are called "df_expectation" and "df_reality"
# Declare a tuple with valid primary key, let's say it is called "primary_key"

output = spot_variance(df_expectation, df_reality, primary_key)
output.show()


# to generate variance report
from effulge import save_variance_report
save_variance_report(
    variance_df=output, source_df=df_expectation, target_df=df_reality,
    super_key=primary_key, file_name="effulge_variance_report",
    src_prefix='SRC', tgt_prefix='TGT'
)

```
-----
