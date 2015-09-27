/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;

        MemberListEntry* own_node = new MemberListEntry();
        own_node->setid(memberNode->addr.getId());
        own_node->setport(memberNode->addr.getPort());
        own_node->setheartbeat(memberNode->heartbeat);
        memberNode->memberList.push_back(*own_node);

    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

            MemberListEntry* own_node = new MemberListEntry();
            own_node->setid(memberNode->addr.getId());
            own_node->setport(memberNode->addr.getPort());
            own_node->setheartbeat(memberNode->heartbeat);
            memberNode->memberList.push_back(*own_node);

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
	// V. Cao
	MessageHdr *hdr = (MessageHdr*)data;
	Address *new_node_addr = new Address();
	MessageHdr *reply_msg = new MessageHdr();
	Address* add_addr = new Address();


	switch( hdr->msgType ){
		case JOINREQ:
                  int id;
                  short port;
                  long heart_beat;
                  MemberListEntry* new_member;
                  memcpy(new_node_addr->addr, (void* )(data + sizeof(MessageHdr)), sizeof(memberNode->addr.addr) );
                  memcpy(&heart_beat, (void *)(data + sizeof(MessageHdr) + sizeof(long) + 1) , sizeof(long));

                  memcpy(&id, &new_node_addr->addr[0], sizeof(int));
                  memcpy(&port, &new_node_addr->addr[4], sizeof(short));

                  new_member = new MemberListEntry(id, port, heart_beat, par->getcurrtime());
                  memberNode->memberList.push_back(*new_member);
                  add_addr = new Address(to_string(new_member->getid()) + ":"+ to_string(new_member->getport()));
                  log->logNodeAdd(&memberNode->addr, add_addr );

                  reply_msg->msgType = JOINREP;

                  emulNet->ENsend(&memberNode->addr, new_node_addr, (char *)reply_msg, sizeof(MessageHdr));
                  free(new_node_addr);
                  free(reply_msg);
                  //cout << id  << "was join" << endl;
                  break;
		case JOINREP: // initzalize new Memberlist and add itself node to first emlement
                  memberNode->inGroup = true;


                  ///cout << memberNode->addr.getAddress() << "already recvice JOINREP mgs"<< endl;
                  break;
		case GOSSIP:
                  {
                  if(  !memberNode->inGroup ) break;
                  //cout << sizeof(data)<<"       "<< sizeof(MessageHdr)<< endl;
                  vector<MemberListEntry> Comming_list;
                  Comming_list.clear();
                  int entry_count = (size - sizeof(MessageHdr)) / sizeof(MemberListEntry);
                  for(int i = 0; i < entry_count; i++)
                  {
                        Comming_list.push_back(*(MemberListEntry *)( data + sizeof(MessageHdr) + i*sizeof(MemberListEntry)));
                  }
                  //vector<MemberListEntry> Comming_list = (vector<MemberListEntry> )*(data + sizeof(MessageHdr) );

                  cout << "Node "<< memberNode->addr.getAddress() << " was recvied GOSSIP with size :  "<< Comming_list.size() << " when have size"<< memberNode->memberList.size() << endl;

                  // search send node on current list and update it no matter it marked falied or not.
                  MemberListEntry send_node = Comming_list.front();
                  int i;
                  for( i = 1 ; i < memberNode->memberList.size(); i++  )
                  {

                       // MemberListEntry old_entry = memberNode->memberList.at(i);
                        vector<MemberListEntry>::iterator it = memberNode->memberList.begin() + i;
                        if( send_node.getid() == it->getid() )
                        {

                              if( send_node.getheartbeat() > it->getheartbeat() ){
                                    it->setheartbeat(send_node.getheartbeat());
                                    it->settimestamp(par->getcurrtime());
                              }
                              break;
                        }

                  }
                  if( i >= (int)memberNode->memberList.size() )
                  {
                         cout <<"                                                                       " << i  << endl;
                        memberNode->memberList.push_back(send_node);
                        add_addr = new Address(to_string(send_node.getid()) + ":"+ to_string(send_node.getport()));
                        log->logNodeAdd(&memberNode->addr, add_addr );
                  }
                  // forget about sendnode just search the rest. and node search ownnode.

                 // cout << Comming_list.size() << " comming's size !"<< endl;

                  for(int i = 1; i < Comming_list.size();  i++)
                  my_label:
                  {
                        cout << "can   i   = "<< i << endl;
                        MemberListEntry new_entry = Comming_list.at(i);
                        for(int j = 0 ; j < memberNode->memberList.size(); j++)
                        {
                              //MemberListEntry old_entry = memberNode->memberList.at(j);
                              vector<MemberListEntry>::iterator it = memberNode->memberList.begin() + j;
                              if( new_entry.getid() == it->getid())
                              {

                                    // heart_beat < 0 used to mark node faled
                                    if(new_entry.getheartbeat() > it->getheartbeat())
                                    {
                                          if(new_entry.getheartbeat() >= 0 && it->getheartbeat() >= 0 )
                                          {
                                                //old_entry = new_entry;
                                                it->setheartbeat(new_entry.getheartbeat());
                                                it->settimestamp( par->getcurrtime());

                                          }
                                    }
                                    //cout << "                                : "<< i << endl;
                                    i++;
                                    if( i < Comming_list.size() )   goto my_label;
                                    else goto out;
                              }


                        }
                         // if old list don't have entry in new list then add new_entry
                        memberNode->memberList.push_back(new_entry);
                        add_addr = new Address(to_string(new_entry.getid()) + ":"+ to_string(new_entry.getport()));
                        log->logNodeAdd(&memberNode->addr, add_addr );
                        cout << "push back "<< new_entry.getid() << " : at orginal node "<< memberNode->memberList.at(0).getid() << endl;


                  }
                  out:
                  //cout << memberNode->addr.getAddress() << " have "<< memberNode->memberList.size() << " entry "<< endl;
                  break;
                  }
            /*
            case FAILDETEC:
                  Address* fail_addr = (Address *)(data + sizeof(MessageHdr));
                  for(int i = 0; i < memberNode->memberList.size(); i++ )
                  {
                        vector<MemberListEntry>::iterator it = memberNode->memberList.begin() + i;
                        if( it->getid() == fail_addr->getId())
                        {
                              log->logNodeRemove(&memberNode->addr, fail_addr); // print removed to log
                              if( i != 0 )
                                    memberNode->memberList.erase(it);
                              //it->settimestamp( par->getcurrtime() - TFAIL - 1);
                              //it->setheartbeat(-1);
                              MessageHdr* msg;
                              size_t msgsize = sizeof(MessageHdr) + sizeof(Address);
                              msg = (MessageHdr *) malloc(msgsize * sizeof(char));
                              msg->msgType = FAILDETEC;
                              memcpy((char *)msg + sizeof(MessageHdr), fail_addr, sizeof(Address));

                              int vector_size = memberNode->memberList.size();
                              int revciver_1st, revciver_2sd; // send 1st node in first hafl list and 2sd node to second hatf, the midle node may be double send
                              revciver_1st = random() % vector_size/2 + 1; // +1 to avoid send to itself
                              revciver_2sd = revciver_1st + vector_size/2;

                              if( revciver_1st < vector_size)
                              {
                                    Address* recv_1st_addr = new Address(to_string(memberNode->memberList.at(revciver_1st).getid())+":"+to_string(memberNode->memberList.at(revciver_1st).getport()));

                                    emulNet->ENsend(&memberNode->addr, recv_1st_addr, (char *)msg, (int )msgsize);
                                   // cout << revciver_1st << endl;
                                    //cout << memberNode->addr.getAddress() << " send GOSSIP to : " << memberNode->memberList.at(revciver_1st).getid() <<" with size "<< vector_size << endl;
                              }
                              if( revciver_2sd < vector_size)
                              {
                                    Address* recv_2sd_addr = new Address(to_string(memberNode->memberList.at(revciver_2sd).getid())+":"+to_string(memberNode->memberList.at(revciver_2sd).getport()));
                                    emulNet->ENsend(&memberNode->addr, recv_2sd_addr, (char *)msg, (int )msgsize);
                                    //cout << revciver_2sd << endl;
                                   // cout << memberNode->addr.getAddress() << " send GOSSIP to : " << memberNode->memberList.at(revciver_2sd).getid() <<" with size "<< vector_size << endl;
                              }
                        }
                  }
                  break;
                  */
      }
}
/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
      memberNode->heartbeat++;
      vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
      it->setheartbeat(memberNode->heartbeat);

      for(int i = 1 ; i < memberNode->memberList.size(); i++)
      {
            MemberListEntry entry = memberNode->memberList.at(i);
            if( (par->getcurrtime() - entry.gettimestamp()) > TFAIL )
            {
                  //cout << " notice failed happpen" << endl;
                  //cout <<" Heartbeat befor test :           " <<entry.getheartbeat() << endl;
                  if( entry.getheartbeat() < 0 ) // it mean that node already make be failed
                  {
                        //cout << "               remove node " << endl;
                        MemberListEntry remove_node = memberNode->memberList.at(i);
                        //if( i != 0){
                        memberNode->memberList.erase(memberNode->memberList.begin()+ i);
                       // }

                        Address* remove_addr = new Address(to_string(remove_node.getid())+ ":"+ to_string(remove_node.getport()));
                        /*
                        MessageHdr* msg;
                        size_t msgsize = sizeof(MessageHdr) + sizeof(Address);
                        msg = (MessageHdr *) malloc(msgsize * sizeof(char));
                        msg->msgType = FAILDETEC;
                        memcpy((char *)msg + sizeof(MessageHdr), remove_addr, sizeof(Address));

                        int vector_size = memberNode->memberList.size();
                        int revciver_1st, revciver_2sd; // send 1st node in first hafl list and 2sd node to second hatf, the midle node may be double send
                        revciver_1st = random() % vector_size/2 + 1; // +1 to avoid send to itself
                        revciver_2sd = revciver_1st + vector_size/2;

                        if( revciver_1st < vector_size)
                        {
                              Address* recv_1st_addr = new Address(to_string(memberNode->memberList.at(revciver_1st).getid())+":"+to_string(memberNode->memberList.at(revciver_1st).getport()));

                              emulNet->ENsend(&memberNode->addr, recv_1st_addr, (char *)msg, (int )msgsize);
                             // cout << revciver_1st << endl;
                              //cout << memberNode->addr.getAddress() << " send GOSSIP to : " << memberNode->memberList.at(revciver_1st).getid() <<" with size "<< vector_size << endl;
                        }
                        if( revciver_2sd < vector_size)
                        {
                              Address* recv_2sd_addr = new Address(to_string(memberNode->memberList.at(revciver_2sd).getid())+":"+to_string(memberNode->memberList.at(revciver_2sd).getport()));
                              emulNet->ENsend(&memberNode->addr, recv_2sd_addr, (char *)msg, (int )msgsize);
                              //cout << revciver_2sd << endl;
                             // cout << memberNode->addr.getAddress() << " send GOSSIP to : " << memberNode->memberList.at(revciver_2sd).getid() <<" with size "<< vector_size << endl;

                        }
                        */


                        log->logNodeRemove(&memberNode->addr, remove_addr);
                        /*
                        if( i != 0){
                              log->log( remove_addr, "node failed at time = %d", par->getcurrtime());
                        }
                        */
                  }
                  else
                  {
                        //entry.setheartbeat( 0 - entry.getheartbeat()); // mark node failed

                        vector< MemberListEntry>::iterator it = memberNode->memberList.begin() + i;


                        it->setheartbeat(0 - entry.getheartbeat());
                        //it.setheartbeat(0 - entry.getheartbeat());
                        cout << "marked was fail and heartbeat set to : "<< entry.getheartbeat() << endl;
                        cout << "After set heartbeat :      "<< entry.getheartbeat() << endl;
                  }
            }
      }
      int vector_size = memberNode->memberList.size();
      int revciver_1st, revciver_2sd; // send 1st node in first hafl list and 2sd node to second hatf, the midle node may be double send
      revciver_1st = random() % vector_size/2 + 1; // +1 to avoid send to itself
      revciver_2sd = revciver_1st + vector_size/2;
     // cout << memberNode->    addr.getAddress()   <<"    "<<list_size << "   entrys at round "<< par->getcurrtime() << endl;
      //cout << list_size <<"   "<< revciver_1st<< "    "<< revciver_2sd<< endl;
      //Address* recv_1st_addr = new Address(to_string(memberNode->memberList.at(revciver_1st).getid())+":"+to_string(memberNode->memberList.at(revciver_1st).getport()));
      //Address* recv_2sd_addr = new Address(to_string(memberNode->memberList.at(revciver_2sd).getid())+":"+to_string(memberNode->memberList.at(revciver_2sd).getport()));

        //cout <<"size of memberlist " <<(int )memberNode->memberList.capacity() << endl;
      MessageHdr* msg;
      size_t msgsize = sizeof(MessageHdr) + vector_size* sizeof(MemberListEntry);
     // cout << memberNode->memberList.capacity() <<"byte memberlist   ";
      //cout<< "Message size : "<< msgsize << endl;
      msg = (MessageHdr *) malloc(msgsize * sizeof(char));
      msg->msgType = GOSSIP;
      //memcpy((char *)(msg+1), &memberNode->memberList, (int)memberNode->memberList.capacity() );
      for( int i = 0; i < vector_size; i++)
      {
            memcpy((char*)((char* )msg + sizeof(MessageHdr) + i*sizeof(MemberListEntry)), &memberNode->memberList.at(i), sizeof(MemberListEntry) );
      }



      if( revciver_1st < vector_size)
      {
            Address* recv_1st_addr = new Address(to_string(memberNode->memberList.at(revciver_1st).getid())+":"+to_string(memberNode->memberList.at(revciver_1st).getport()));

            emulNet->ENsend(&memberNode->addr, recv_1st_addr, (char *)msg, (int )msgsize);
            cout << revciver_1st << endl;
            cout << memberNode->addr.getAddress() << " send GOSSIP to : " << memberNode->memberList.at(revciver_1st).getid() <<" with size "<< vector_size << endl;
      }
      if( revciver_2sd < vector_size)
      {
            Address* recv_2sd_addr = new Address(to_string(memberNode->memberList.at(revciver_2sd).getid())+":"+to_string(memberNode->memberList.at(revciver_2sd).getport()));
            emulNet->ENsend(&memberNode->addr, recv_2sd_addr, (char *)msg, (int )msgsize);
            cout << revciver_2sd << endl;
            cout << memberNode->addr.getAddress() << " send GOSSIP to : " << memberNode->memberList.at(revciver_2sd).getid() <<" with size "<< vector_size << endl;

      }
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;
}
