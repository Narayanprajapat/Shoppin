import requests
import jsonlines
from typing import Optional
from pydantic import BaseModel
from concurrent.futures import ThreadPoolExecutor, as_completed


class Post(BaseModel):
    userId: int
    id: int
    title: str
    body: str


class Comment(BaseModel):
    postId: int
    id: int
    name: str
    email: str
    body: str


class Album(BaseModel):
    userId: int
    id: int
    title: str


class Photo(BaseModel):
    albumId: int
    id: int
    title: str
    url: str
    thumbnailUrl: str


class Address(BaseModel):
    street: str
    suite: str
    city: str
    zipcode: str
    geo: Optional[dict]


class Company(BaseModel):
    name: str
    catchPhrase: str
    bs: str


class User(BaseModel):
    id: int
    name: str
    username: str
    email: str
    address: Address
    phone: str
    website: str
    company: Company


def fetch_and_validate(url, model) -> list:
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        validated_data = [model(**item).dict() for item in data]
        return validated_data
    except Exception as e:
        print(f"Error fetching or validating data from {url}: {e}")
        return []


def save_to_jsonlines(data, filename):
    print(filename, data)
    with jsonlines.open(filename, mode="a") as writer:
        writer.write_all(data)


def main() -> None:
    endpoints = {
        "posts": ("https://jsonplaceholder.typicode.com/posts", Post),
        "comments": ("https://jsonplaceholder.typicode.com/comments", Comment),
        "albums": ("https://jsonplaceholder.typicode.com/albums", Album),
        "photos": ("https://jsonplaceholder.typicode.com/photos", Photo),
        "users": ("https://jsonplaceholder.typicode.com/users", User),
    }

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_endpoint = {
            executor.submit(fetch_and_validate, url, model): name
            for name, (url, model) in endpoints.items()
        }

        for future in as_completed(future_to_endpoint):
            name = future_to_endpoint[future]
            try:
                data = future.result()
                if data:
                    filename = f"./{name}.jsonl"
                    save_to_jsonlines(data, filename)

            except Exception as e:
                print(f"Error processing {name}: {e}")


if __name__ == "__main__":
    main()
