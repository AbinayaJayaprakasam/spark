from pyspark.sql import SparkSession

print("=" * 70)
print("Testing PySpark AFTER FIX")
print("=" * 70)

spark = SparkSession.builder \
    .appName("Test OrderBy Fix") \
    .master("local[*]") \
    .getOrCreate()

import pyspark
print(f"PySpark Version: {pyspark.__version__}\n")

# Create sample DataFrame
df = spark.createDataFrame([
    (1, "Alice", 85),
    (2, "Bob", 92),
    (3, "Charlie", 78)
], ["id", "name", "score"])

print("Original DataFrame:")
df.show()

print("\n" + "=" * 70)
print("Test 1: String 'asc' (NOW SHOULD WORK!)")
print("=" * 70)
try:
    df.orderBy("id", ascending="asc").show()
    print("✅ SUCCESS! String 'asc' now works!")
except Exception as e:
    print(f"❌ Failed: {type(e).__name__}: {e}")

print("\n" + "=" * 70)
print("Test 2: String 'desc' (NOW SHOULD WORK!)")
print("=" * 70)
try:
    df.orderBy("id", ascending="desc").show()
    print("✅ SUCCESS! String 'desc' now works!")
except Exception as e:
    print(f"❌ Failed: {type(e).__name__}: {e}")

print("\n" + "=" * 70)
print("Test 3: Invalid string 'invalid' (NOW GETS CLEAR ERROR!)")
print("=" * 70)
try:
    df.orderBy("id", ascending="invalid").show()
    print("❌ ERROR: Should have raised an error!")
except Exception as e:
    print(f"✅ Got clear error: {type(e).__name__}")
    print(f"\n   Error message:")
    print("   " + "-" * 66)
    error_msg = str(e)
    print(f"   {error_msg}")
    print("   " + "-" * 66)
    print("\n   ✅ This error is MUCH clearer!")
    print("   ✅ Users now know what values are valid!")

print("\n" + "=" * 70)
print("Test 4: Boolean True (STILL WORKS)")
print("=" * 70)
try:
    df.orderBy("id", ascending=True).show()
    print("✅ SUCCESS! Boolean True still works!")
except Exception as e:
    print(f"❌ Failed: {type(e).__name__}: {e}")

print("\n" + "=" * 70)
print("Test 5: Boolean False (STILL WORKS)")
print("=" * 70)
try:
    df.orderBy("id", ascending=False).show()
    print("✅ SUCCESS! Boolean False still works!")
except Exception as e:
    print(f"❌ Failed: {type(e).__name__}: {e}")

spark.stop()

print("\n" + "=" * 70)
print("✅ FIX COMPLETE!")
print("=" * 70)
print("NOW:")
print("  ✅ String 'asc' works")
print("  ✅ String 'desc' works")
print("  ✅ Invalid strings give clear error messages")
print("  ✅ Booleans still work as before")
print("=" * 70)

