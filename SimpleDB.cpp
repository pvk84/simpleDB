//
//  simpleDB.cpp
//  simpleDB
//
//  Created by vamsi on 2/15/16.
//  Copyright © 2016 VamsiKrishna. All rights reserved.
//

#include "SimpleDB.hpp"

#include <iostream>
#include <sstream>
#include <algorithm>
#include <memory>

namespace COMMAND{
    std::string UNKNOWN = "UNKNOWN";
    std::string SET = "SET";
    std::string GET = "GET";
    std::string UNSET = "UNSET";
    std::string NUMEQUALTO = "NUMEQUALTO";
    std::string END = "END";
    
    std::string BEGIN = "BEGIN";
    std::string COMMIT = "COMMIT";
    std::string ROLLBACK = "ROLLBACK";
};


SimpleDB::SimpleDB(){
    
}

SimpleDB::~SimpleDB(){
    
}

void SimpleDB::set(std::string key, std::string val){
    if(key.empty()){
        // error
        return;
    }
    std::string prev;
    if(mDB.find(key) != mDB.end()){
        prev = mDB[key];
        mCountMap[prev]--;
    }else{
        prev = "NULL";
    }
    //std::cout << std::boolalpha << mTransactionStack.empty() << std::endl;
    if(!mTransactionStack.empty()){
        std::stack<CommandObj>& commandStack = mTransactionStack.top();
        commandStack.push(std::make_tuple(COMMAND::SET, key, prev));
    }
    
    mDB[key] = val;
    mCountMap[val]++;
}

std::string SimpleDB::get(std::string key){
    if(key.empty()){
        // error
        return "NULL";
    }
    if(mDB.find(key) != mDB.end()){
        return mDB[key];
    }else{
        return "NULL";
    }
}

bool SimpleDB::unset(std::string key){
    if(mDB.find(key) != mDB.end()){
        std::string prev = mDB[key];
        mCountMap[prev]--;
        if(!mTransactionStack.empty()){
            std::stack<CommandObj>& commandStack = mTransactionStack.top();
            commandStack.push(std::make_tuple(COMMAND::SET, key, prev));
        }
    }
    size_t deleted = mDB.erase(key);
    if(deleted == 1){
        return true;
    }
    return false;
}

int SimpleDB::numEqualTo(std::string val){
    if(mCountMap.find(val) != mCountMap.end()){
        return mCountMap[val];
    }
    return 0;
}


void SimpleDB::beginTransaction(){
    CommandStack cs;
    mTransactionStack.push(cs);
}

bool SimpleDB::commitTransaction(){
    if (mTransactionStack.empty()) {
        return false;
    }
    while (!mTransactionStack.empty()) {
        mTransactionStack.pop();
    }
    return true;
}

bool SimpleDB::rollbackTransaction(){
    if (mTransactionStack.empty()) {
        return false;
    }
    std::stack<CommandObj> commandStack = mTransactionStack.top();
    mTransactionStack.pop();
    while(!commandStack.empty()) {
        CommandObj obj = commandStack.top();
        commandStack.pop();
        this->execute(obj);
    }
    
    return true;
}

void SimpleDB::execute(CommandObj commandObj){
    std::string command, key, value;
    std::tie(command, key, value) = commandObj;
    //std::cout << command << ", " << key << ", " << value << std::endl;
    
    if (command == COMMAND::SET) {
        if(value == "NULL"){
            this->unset(key);
        }else{
            this->set(key, value);
        }
    }else{
        // will not come here?
    }
}


void SimpleDB::run(){
    
    std::cout << "SimpleDB program begin: " << std::endl;
    
    std::unique_ptr<SimpleDB> simpleDB(new SimpleDB());
    //SimpleDB* simpleDB = new SimpleDB();
    int stop = false;
    
    while (true) {
        
        std::string input;
        while (std::getline(std::cin, input)){
            std::string command = COMMAND::UNKNOWN;
            std::string key = "invalid";
            std::string value = "";
            
            std::istringstream istream(input);
            
            istream >> command;
            //command = std::toupper(command);
            //std::cout << command << ", " << key << ", " << value << std::endl;
            
            if(command == COMMAND::SET){
                istream >> key >> value;
                simpleDB->set(key, value);
            }else if(command == COMMAND::GET){
                istream >> key;
                std::string valStr = simpleDB->get(key);
                std::cout << valStr << std::endl;
            }else if(command == COMMAND::UNSET){
                istream >> key;
                simpleDB->unset(key);
            }else if(command == COMMAND::NUMEQUALTO){
                istream >> value;
                int val = simpleDB->numEqualTo(value);
                std::cout << val << std::endl;
            }else if(command == COMMAND::END){
                stop = true;
                break;
            }else if(command == COMMAND::BEGIN){
                simpleDB->beginTransaction();
            }else if(command == COMMAND::COMMIT){
                if(!simpleDB->commitTransaction()){
                    std::cout << "NO TRANSACTION" << std::endl;
                }
            }else if(command == COMMAND::ROLLBACK){
                if(!simpleDB->rollbackTransaction()){
                    std::cout << "NO TRANSACTION" << std::endl;
                }
            }else{
                std::cout << "Unknown command!" << std::endl;
            }
            istream.clear();
            input = "";
            
        }
        
        if(stop){
            break;
        }
    }
    
    //delete simpleDB;
    
}