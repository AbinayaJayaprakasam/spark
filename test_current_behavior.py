from pyspark.sql import SparkSession

print("=" * 70)
print("Testing CURRENT PySpark behavior (BEFORE fix)")
print("=" * 70)

spark = SparkSession.builder \
    .appName("Test OrderBy Error") \
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
print("Test 1: Boolean True (CURRENTLY WORKS)")
print("=" * 70)
try:
    df.orderBy("id", ascending=True).show()
    print("✅ Success with ascending=True")
except Exception as e:
    print(f"❌ Failed: {type(e).__name__}: {e}")

print("\n" + "=" * 70)
print("Test 2: Boolean False (CURRENTLY WORKS)")
print("=" * 70)
try:
    df.orderBy("id", ascending=False).show()
    print("✅ Success with ascending=False")
except Exception as e:
    print(f"❌ Failed: {type(e).__name__}: {e}")

print("\n" + "=" * 70)
print("Test 3: String 'asc' (CURRENTLY FAILS - Need to add support)")
print("=" * 70)
try:
    df.orderBy("id", ascending="asc").show()
    print("✅ Success with ascending='asc'")
except Exception as e:
    print(f"❌ Current Error: {type(e).__name__}")
    print(f"   Message: {str(e)[:150]}...")
    print("   This is the problem we're fixing!")

print("\n" + "=" * 70)
print("Test 4: String 'desc' (CURRENTLY FAILS - Need to add support)")
print("=" * 70)
try:
    df.orderBy("id", ascending="desc").show()
    print("✅ Success with ascending='desc'")
except Exception as e:
    print(f"❌ Current Error: {type(e).__name__}")
    print(f"   Message: {str(e)[:150]}...")
    print("   This is the problem we're fixing!")

print("\n" + "=" * 70)
print("Test 5: String 'invalid' (CURRENTLY GIVES UNCLEAR ERROR)")
print("=" * 70)
try:
    df.orderBy("id", ascending="invalid").show()
    print("❌ ERROR: Should have failed!")
except Exception as e:
    print(f"❌ Current Error: {type(e).__name__}")
    print(f"\n   Error message:")
    print("   " + "-" * 66)
    error_msg = str(e)
    print(f"   {error_msg[:200]}")
    if len(error_msg) > 200:
        print("   ... [truncated]")
    print("   " + "-" * 66)
    print("\n   🔴 This error is confusing and unclear!")
    print("   🔴 Users don't know 'asc' and 'desc' are valid!")
    print("   🔴 After fix, this will say: 'ascending must be True, False, 'asc', or 'desc''")

spark.stop()
print("\n" + "=" * 70)
print("SUMMARY:")
print("=" * 70)
print("BEFORE FIX:")
print("  ❌ String 'asc' and 'desc' don't work")
print("  ❌ Error messages are unclear")
print("\nAFTER FIX:")
print("  ✅ String 'asc' and 'desc' will work")
print("  ✅ Invalid strings get clear error messages")
print("=" * 70)