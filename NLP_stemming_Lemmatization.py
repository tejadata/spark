# -*- coding: utf-8 -*-
"""
Created on Thu Feb 28 18:44:13 2019

@author: vishwateja.r
"""

import nltk as nlp
import re
from nltk.stem import PorterStemmer
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords

data  = """The award goes to mr. Teja,When I was ther, I went there goes by going"""

def remove_stopwords(stop_words):
    # we are using for loop to loop into each sentence
    for i in range(len(stop_words)): 
        # Now each sentence we are tokenizing to words
        words = nlp.word_tokenize(stop_words[i]) 
        #Now removing the stop words using list comprehensive looping the word tokenize and cheking if stop words are present
        words = [word for word in words if word not in stopwords.words('english')] 
        # using join methods we are converting list of words into string
        stop_words[i] = ' '.join(words)
    return stop_words
                   

def steamlemma():
    #sentence tokenize 
    sent_token=nlp.sent_tokenize(data)
    
    stemmer = PorterStemmer()
    
    #sentence tokenize 
    sent_token_steming=nlp.sent_tokenize(data)
    sent_token_steming = remove_stopwords(sent_token_steming)
    
    for i in range(len(sent_token)):
        words = nlp.word_tokenize(sent_token[i])
        words = [stemmer.stem(word) for word in words]
        sent_token_steming[i] = ' '.join(words)
    
    
    lemmat = WordNetLemmatizer()
    
    #sentence tokenize and stop words
    sent_token_lemma=nlp.sent_tokenize(data)
    sent_token_lemma = remove_stopwords(sent_token_lemma)
    
    for i in range(len(sent_token)):
        words = nlp.word_tokenize(sent_token[i])
        words = [lemmat.lemmatize(word) for word in words]
        sent_token_lemma[i] = ' '.join(words)
    
    return str(sent_token_lemma),str(sent_token_steming)    

        
if __name__ == '__main__':
    res=steamlemma()
    print(res)
