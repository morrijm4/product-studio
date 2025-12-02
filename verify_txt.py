from google.transit import gtfs_realtime_pb2
file_path = "response2.bin"  # Replace with the actual path to your file

try:
    with open(file_path, 'rb') as f:   # <-- binary mode
        content = f.read()
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(content)

    print("Parsed OK!")
    print("Header:", feed.header)
    print("Number of entities:", len(feed.entity))
    print(feed)

except FileNotFoundError:
    print(f"Error: The file '{file_path}' was not found.")
except Exception as e:
    print(f"An error occurred: {e}")