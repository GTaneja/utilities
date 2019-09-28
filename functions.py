# source: https://stackoverflow.com/questions/44327999/python-pandas-merge-multiple-dataframes/44338256

def merge_dfs(data_frames,**kwargs):
    import reduce
    import pandas as pd
    
    jointype = kwargs.get('how','outer')
    byvars = kwargs.get('byvars','')
    nastring = kwarns.get('nastring','')
    if byvars=='':
        raise ValueError("Need a variable(s) to merge on")
    elif (isinstance(byvars,list):
        if len(data_frames)==1:
            riase ValueError("More than one data frames in list require to merge")
        if (len(byvars)>1) & (len(byvars)!=len(data_frames)-1):
            raise ValueError("number of by variables not same as number of joins being attempted {0}!={1}".format(len(byvars),len(data_frames)-1))
        elif (len(byvars)==1) & (len(data_frames)>1):
            byvars = [byvars]*(len(data_frames)-1)
    
    
    # if you want to fill the values that don't exist in the lines of merged dataframe simply fill with required strings as
    df_merged = reduce(lambda  left,right: pd.merge(left,right,on=byvars,how=jointype), data_frames).fillna(nastring)
    print("Merged df: {0} Listed dfs: {1}".format(df_merged.shape, [ d.shape for d in data_frames])
    return df_merged
