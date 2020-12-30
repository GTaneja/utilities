# Joining tables
# source: https://stackoverflow.com/questions/44327999/python-pandas-merge-multiple-dataframes/44338256
# :data_frames: :list of pandas data frames:
# :byvars: :list of size 1 with the element as string name of id variable in all datasets:
def merge_dfs(data_frames,**kwargs):
    from functools import reduce
    import pandas as pd
    
    jointype = kwargs.get('how','outer')
    byvars = kwargs.get('byvars','')
    nastring = kwargs.get('nastring','')
    verbose = kwargs.get('verbose',False)
    if verbose:
      print("You passed {0} data frames\n Columns : {1}\n Shape: {2}".format(len(data_frames),[d.columns for d in data_frames], [d.shape for d in data_frames]))
    if byvars=='':
        raise ValueError("Need a variable(s) to merge on")
    elif (isinstance(byvars,list)):
        if (len(data_frames)==1):
            raise ValueError("More than one data frames in list require to merge")
        if (len(byvars)>1) & (len(byvars)!=len(data_frames)-1):
            raise ValueError("number of by variables not same as number of joins being attempted {0}!={1}".format(len(byvars),len(data_frames)-1))    
    
    # if you want to fill the values that don't exist in the lines of merged dataframe simply fill with required strings as
    df_merged = reduce(lambda  left,right: pd.merge(left,right,on=byvars,how=jointype, suffixes=['','_y']), data_frames).fillna(nastring)
    print("Merged df: {0} Listed dfs: {1}".format(df_merged.shape, [ d.shape for d in data_frames]))
    return df_merged

# Function to create windows of words for a CBOW model
# w: list of strings
# c: int
# Example Use: cbow_windows(['i', 'am', 'happy', 'because', 'i', 'am', 'learning'], 2)
def cbow_window(w, c):
    i = c
    while i < len(w) - c:
        center_w = w[i]
        context_w = w[(i - c):i] + w[(i+1):(i+c+1)]
        yield context_w, center_w
        i += 1

# Using gensim
def wrd2vec(w, c):
    from gensim.models import Word2Vec
    word2vec = Word2Vec(all_words=w, min_count=c)
    return word2vec

