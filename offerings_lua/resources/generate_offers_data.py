import json
import random
import argparse
import sys
from tqdm import tqdm
from bson import ObjectId

# --- Constants ---
# Define the three types of offers we can create
OFFER_TYPES = ["BOTH", "GLOBAL_ONLY", "USER_ONLY"]

# Define the value ranges for the caps
GLOBAL_CAP_MIN, GLOBAL_CAP_MAX = 500, 100000
USER_CAP_MIN, USER_CAP_MAX = 1, 10


def generate_offers_data(num_offers):
    """
    Generates a dictionary of offers, with offer_id as the key.
    The three offer types are distributed evenly.

    Args:
        num_offers (int): The total number of offers to create.

    Returns:
        A dictionary containing the generated offers.
    """
    print(f"Generating {num_offers} offers as a single JSON object...")

    all_offers_dict = {}

    for i in tqdm(range(num_offers)):
        # --- 1. Generate a unique ID for the offer ---
        offer_id = str(ObjectId())

        # --- 2. Cycle through the offer types for even distribution ---
        current_type = OFFER_TYPES[i % len(OFFER_TYPES)]

        # --- 3. Build the offer details based on the type ---
        offer_details = {"type": current_type}

        if current_type == "BOTH":
            offer_details["global_cap"] = random.randint(GLOBAL_CAP_MIN, GLOBAL_CAP_MAX)
            offer_details["user_cap"] = random.randint(USER_CAP_MIN, USER_CAP_MAX)

        elif current_type == "GLOBAL_ONLY":
            offer_details["global_cap"] = random.randint(GLOBAL_CAP_MIN, GLOBAL_CAP_MAX)

        elif current_type == "USER_ONLY":
            offer_details["user_cap"] = random.randint(USER_CAP_MIN, USER_CAP_MAX)

        # --- 4. Add the complete offer to our main dictionary ---
        all_offers_dict[offer_id] = offer_details

    return all_offers_dict


def main():
    """Main function to parse arguments and run the generator."""
    parser = argparse.ArgumentParser(
        description="Generate a large-scale JSON file with offers as a single object.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "num_offers",
        type=int,
        help="The total number of offers to generate."
    )
    parser.add_argument(
        "-o", "--output",
        default="offers_db.json",
        help="Path to the output JSON file (default: offers_object.json)."
    )
    # Note: Streaming is not applicable here as we are creating a single JSON object,
    # which must be held in memory before being written to a file.

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()

    try:
        # Generate the entire dictionary in memory
        offers_data = generate_offers_data(args.num_offers)

        print(f"Writing data to {args.output}...")
        with open(args.output, 'w') as f:
            # Dump the entire dictionary with indentation for readability
            json.dump(offers_data, f, indent=2)

        print(f"\n✅ Successfully generated {args.num_offers} offers in '{args.output}'.")

    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
    # python generate_offers_data.py 1000