import sys
import pandas as pd
from sqlalchemy import create_engine


def load_data(messages_filepath, categories_filepath):
    '''load raw data from the given file path
    
    Args:
    messages_filepath: string. Filepath for messsage content data
    categories_filepath: string. Filepath for message category data

    Returns:
    df: the dataframe of message data and categories data merged
    '''
    messages = pd.read_csv(messages_filepath)
    categories = pd.read_csv(categories_filepath)
    df = messages.merge(categories, on='id')
    return df


def clean_data(df):
    '''Clean the merged message and category data by building correct features/target

    Args: df: dataframe of merged message and category data

    Returns:
    cleaned_df: cleaned data
    '''
    #split the category string and expand columns
    categories = df.categories.str.split(";", expand=True) 

    # extract a list of new column names for categories.
    row = categories.iloc[0,:]
    category_colnames = [(lambda string : string[:-2])(category) for category in row]
    categories.columns = category_colnames

    #Iterate through the category columns in df to keep only the last character of each string (the 1 or 0)
    for column in categories:
    # set each value to be the last character of the string
        categories[column] = categories[column].astype("string").str[-1]
    
    # convert column from string to numeric
    categories[column] = categories[column].astype('int')
    
    #drop the original categories column in df and replace it with expanded category data
    df = df.drop('categories', axis=1)
    df = pd.concat([df, categories], axis = 1)

    # drop duplicates
    cleaned_df = df.drop_duplicates()
    return cleaned_df


def save_data(df, database_filename):
    '''Save the cleaned data into a sql database

    Args:
    df: dataframe to be saved
    database_filename: string name of the database

    Returns: 
    None
    
    ''' 
    engine = create_engine('sqlite:///database_filename.db')
    df.to_sql('messages_cleaned', engine, index=False)
    return None


def main():

    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)
        
        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)
        
        print('Cleaned data saved to database!')
    
    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()