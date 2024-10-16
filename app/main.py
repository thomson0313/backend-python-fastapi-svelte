import pandas as pd
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Allow all origins for now (use specific origins in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

crypto_history_df = pd.read_csv(r"data/crypto_history.csv")
twitter_user_df = pd.read_csv(r"data/twitter_user.csv",dtype=str)
twitter_post_df = pd.read_csv(r"data/twitter_post.csv",dtype=str).fillna("")
crypto_token_df = pd.read_csv(r"data/crypto_token.csv",dtype=str)


# Endpoint to query twitter_user data
@app.get("/")
def default():

    return {"Hello, World!"}


# Endpoint to query twitter_user data
@app.get("/twitter_user/")
def query_twitter_user(
    username: str = Query(None),
):
    """
    Filters the twitter_user_df DataFrame based on the provided parameters.

    :param username: Filter by username (substring match).
    :param follower_count_min: Filter by minimum follower count.
    :param verified: Filter by verification status (True/False).
    :return: Filtered data as a list of dictionaries.
    """
    print(username)
    filtered_df = twitter_user_df.copy()

    if username:
        filtered_df = filtered_df[
            filtered_df["username"].str.contains(username, case=False)
        ].head(10)
        print(filtered_df)
        return filtered_df.to_dict(orient="records")

    else:
        return filtered_df.to_dict(orient="records")


@app.get("/twitter_post/")
def query_twitter_post(
    user_id: str = Query(None), token_symbol: str = Query(None), date: str = Query(None)
):
    # Load the Twitter posts data

    # Apply filters
    filtered_df = twitter_post_df.copy()

    if user_id:
        filtered_df = filtered_df[filtered_df["user_id"] == user_id]

    if token_symbol:
        # Use str.contains for a case-insensitive partial match
        filtered_df = filtered_df[
            filtered_df["token_symbol"].str.contains(token_symbol, case=False, na=False)
        ]

    # Print filtered DataFrame for debugging
    print(filtered_df)

    # Return the filtered data as a dictionary
    return filtered_df.to_dict(orient="records")


# Endpoint to query crypto_token data
@app.get("/crypto_token/")
def query_crypto_token(name: str = Query(None)):

    filtered_df = crypto_token_df.copy()

    if name:
        filtered_df = (
            filtered_df[filtered_df["name"].str.contains(name, case=False)]
            .head(10)
            .fillna("")
        )
        print(filtered_df)
        return filtered_df.to_dict(orient="records")
        
    else:
        print(filtered_df)

        return filtered_df.to_dict(orient="records")


# Endpoint to query crypto_history data
@app.get("/crypto_history/")
def query_crypto_history(token_name: str = Query(None), date: str = Query(None)):


    filtered_df = crypto_history_df.copy()

    if token_name:
        filtered_df = filtered_df[
            filtered_df["token_name"].str.contains(token_name, case=False)
        ]
        print(len(filtered_df))
        return filtered_df.tail(10000).to_dict(orient="records")
    else: 
        print("no history")