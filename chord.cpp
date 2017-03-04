#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <netdb.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <signal.h>
#include <pthread.h>
#include <thread>
#include <utility>
#include <iostream>
#include <string>
#include <random>
#include <chrono>
#include <map>
#include <unordered_map>

#define succ next[0]
#define BITS 20
#define MOD ((1 << BITS) - 1)
#define rand() dis(gen)
#define uint unsigned int

using namespace std;

int GOODBYE = -1;
int FINDSUCC = 0;
int GETPRED = 1;
int CHPRED = 2;
int CHFINGER = 3;
int ADDFILE = 4;
int NEEDFILE = 5;
int GETCPN = 6;
int GETSUCC = 7;
int GIVEFILE = 8;
int IQUIT = 9;
int TEST = 69;
pair<uint, uint> bug = make_pair(0, 0);

mt19937 gen(time(0));
uniform_int_distribution<int> dis(0, 1 << BITS);
bool inRange(uint, uint, uint);
void treat(int);
void serverThread();
void fixTable();

extern int errno;
int op;
string name;

struct finger {
  uint start, node, port;

  finger(uint _start = 0, uint _node = 0, uint _port = 0) {
    this->start = _start;
    this->node = _node;
    this->port = _port;
  }
};

struct node {
  finger next[BITS];
  uint pred, prport;
  uint poz, myPort;
  map<uint, string> M;

  void init() {
    cout << "PORT: " << flush;
    cin >> this->myPort;
    this->poz = this->SHA(to_string(this->myPort));
    this->pred = this->poz;
    this->prport = this->myPort;
    for(int i = 0; i < BITS; ++i) {
      this->next[i].start = (this->poz + (1 << i)) & MOD;
      this->next[i].node = this->poz;
      this->next[i].port = this->myPort;
    }

    if(this->myPort != 2908) {
      this->getMyFingerTable();
      this->getMyFiles();
    }

    thread startServer(serverThread);
    startServer.detach();
    thread (fixTable).detach();

    cout << "Asignat la pozitia: " << this->poz << endl;
  }

  uint SHA(string s) {
    while(s.size() < 64) s += '0';

    uint word[80];
    memset(word, 0, sizeof(word));

    for(int i = 0; i < 16; ++i)
      for(int j = 3; j >= 0; --j)
        word[i] |= ((int(s[4 * i + 3 - j])) << (8 * j));

    for(int i = 16; i < 80; ++i) {
      uint aux;
      aux = word[i - 3] ^ word[i - 8] ^ word[i - 14] ^ word[i - 16];
      word[i] = ((aux << 1) | (aux >> 31));
    }

    uint h0 = 0x67452301;
    uint h1 = 0xEFCDAB89;
    uint h2 = 0x98BADCFE;
    uint h3 = 0x10325476;
    uint h4 = 0xC3D2E1F0;

    uint a = h0;
    uint b = h1;
    uint c = h2;
    uint d = h3;
    uint e = h4;

    for(int i = 0; i < 80; ++i) {
      uint f, k;

      if(i >= 0 && i < 20) {
        f = ((a & b) | ((~b) and d));
        k = 0x5A827999;
      }

      if(i > 19 && i < 40) {
        f = b ^ c ^ d;
        k = 0x6ED9EBA1;
      }

      if(i > 39 && i < 60) {
        f = ((b & c) | (b & d) | (c & d));
        k = 0x8F1BBCDC;
      }

      if(i > 59 && i < 80) {
        f = b ^ c ^ d;
        k = 0xCA62C1D6;
      }

      uint temp;
      temp = ((a << 5) | (a >> 27)) + f + e + k + word[i];
      e = d;
      d = c;
      c = ((b << 30) | (b >> 2));
      b = a;
      a = temp;
    }

    h0 += a;
    h1 += b;
    h2 += c;
    h3 += d;
    h4 += e;

    return (0LL + h0 + h1 + h2 + h3 + h4) & MOD;
  }

  void getMyFingerTable() {
    int sd;
    sockaddr_in server;
    sd = socket(AF_INET, SOCK_STREAM, 0);
    if(sd == -1)
      return ;
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = htonl(INADDR_ANY);
    server.sin_port = htons(2908);
    if(connect(sd, (sockaddr*)&server, sizeof(sockaddr)) == -1)
      return ;

    pair<uint, uint> aux;
    write(sd, &FINDSUCC, sizeof(FINDSUCC));
    write(sd, &this->succ.start, sizeof(this->succ.start));
    read(sd, &aux, sizeof(aux));
    this->succ.node = aux.first;
    this->succ.port = aux.second;
    write(sd, &GOODBYE, sizeof(GOODBYE));
    close(sd);

    sd = socket(AF_INET, SOCK_STREAM, 0);
    if(sd == -1)
      return ;
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = htonl(INADDR_ANY);
    server.sin_port = htons(this->succ.port);
    if(connect(sd, (sockaddr*)&server, sizeof(sockaddr)) == -1)
      return ;

    write(sd, &GETPRED, sizeof(GETPRED));
    read(sd, &aux, sizeof(aux));
    this->pred = aux.first;
    this->prport = aux.second;

    aux.first = this->poz;
    aux.second = this->myPort;
    write(sd, &CHPRED, sizeof(CHPRED));
    write(sd, &aux, sizeof(aux));
    write(sd, &GOODBYE, sizeof(GOODBYE));
    close(sd);

    for(int i = 1; i < BITS; ++i)
      if(inRange(this->next[i].start, this->poz, this->next[i - 1].node)) {
        this->next[i].node = this->next[i - 1].node;
        this->next[i].port = this->next[i - 1].port;
      } else {
        sd = socket(AF_INET, SOCK_STREAM, 0);
        if(sd == -1)
          return ;
        server.sin_family = AF_INET;
        server.sin_addr.s_addr = htonl(INADDR_ANY);
        server.sin_port = htons(2908);
        if(connect(sd, (sockaddr*)&server, sizeof(sockaddr)) == -1)
          return ;
        write(sd, &FINDSUCC, sizeof(FINDSUCC));
        write(sd, &this->next[i].start, sizeof(this->next[i].start));
        read(sd, &aux, sizeof(aux));
        write(sd, &GOODBYE, sizeof(GOODBYE));
        close(sd);
        this->next[i].node = aux.first;
        this->next[i].port = aux.second;
      }

    if(this->succ.node == this->poz) return;

    sd = socket(AF_INET, SOCK_STREAM, 0);
    if(sd == -1)
      return ;
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = htonl(INADDR_ANY);
    server.sin_port = htons(this->succ.port);
    if(connect(sd, (sockaddr*)&server, sizeof(sockaddr)) == -1)
      return ;

    pair<uint, uint> myself = make_pair(this->poz, this->myPort);
    write(sd, &CHFINGER, sizeof(CHFINGER));
    write(sd, &myself, sizeof(myself));
    write(sd, &GOODBYE, sizeof(GOODBYE));
    close(sd);
  }

  void getMyFiles() {
    if(this->succ.port == this->myPort)
      return;

    int sd;
    sockaddr_in server;
    sd = socket(AF_INET, SOCK_STREAM, 0);
    if(sd == -1)
      return ;
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = htonl(INADDR_ANY);
    server.sin_port = htons(this->succ.port);
    if(connect(sd, (sockaddr*)&server, sizeof(sockaddr)) == -1)
      return ;

    write(sd, &GIVEFILE, sizeof(GIVEFILE));
    write(sd, &this->poz, sizeof(this->poz));
    write(sd, &this->pred, sizeof(this->pred));
    int nr = 0;
    read(sd, &nr, sizeof(nr));
    while(nr--) {
      pair<uint, string> aux = make_pair(0, "");
      read(sd, &aux.first, sizeof(aux.first));
      int len = 0;
      char c = 0;
      read(sd, &len, sizeof(len));
      while(len--) {
        read(sd, &c, sizeof(c));
        aux.second += c;
      }

      this->M[aux.first] = aux.second;
    }

    close(sd);
  }

  pair<uint, uint> findSucc(uint id) {
    if(inRange(id, this->poz + 1, this->succ.node))
      return make_pair(this->succ.node, this->succ.port);

    if(id == this->poz)
      return make_pair(this->poz, this->myPort);

    pair<uint, uint> p = this->closestPrecedingNode(id);
    if(p.second == this->myPort) return make_pair(this->poz, this->myPort);

    int sd;
    sockaddr_in server;
    sd = socket(AF_INET, SOCK_STREAM, 0);
    if(sd == -1)
      return p;
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = htonl(INADDR_ANY);
    server.sin_port = htons(p.second);
    if(connect(sd, (sockaddr*)&server, sizeof(sockaddr)) == -1)
      return p;

    pair<uint, uint> ans;
    write(sd, &FINDSUCC, sizeof(FINDSUCC));
    write(sd, &id, sizeof(id)) ;
    read(sd, &ans, sizeof(ans));
    write(sd, &GOODBYE, sizeof(GOODBYE));
    close(sd);

    return ans;
  }

  pair<uint, uint> closestPrecedingNode(uint id) {
    if(this->poz == this->succ.node)
      return make_pair(this->poz, this->myPort);

    for(int i = BITS - 1; i; --i)
      if(inRange(this->next[i].node, this->poz + 1, id - 1))
        return make_pair(this->next[i].node, this->next[i].port);

    return make_pair(this->succ.node, this->succ.port);
  }

  void fixFinger() {
    int i = 1 + rand() % (BITS - 1);
    pair<uint, uint> aux = this->findSucc(this->next[i].start);
    this->next[i].node = aux.first;
    this->next[i].port = aux.second;
  }

  void stabilize() {
    int sd;
    sockaddr_in server;

    if(this->succ.port == this->myPort) return;

    sd = socket(AF_INET, SOCK_STREAM, 0);
    if(sd == -1)
      return ;
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = htonl(INADDR_ANY);
    if(this->succ.port == this->myPort) return;
    server.sin_port = htons(this->succ.port);
    if(connect(sd, (sockaddr*)&server, sizeof(sockaddr)) == -1)
      return ;

    pair<uint, uint> aux;
    write(sd, &GETPRED, sizeof(GETPRED));
    read(sd, &aux, sizeof(aux));

    if(inRange(aux.first, this->poz + 1, this->succ.node - 1)) {
      this->succ.node = aux.first;
      this->succ.port = aux.second;
    }

    aux = make_pair(this->poz, this->myPort);
    write(sd, &CHPRED, sizeof(CHPRED));
    write(sd, &aux, sizeof(aux));
    write(sd, &GOODBYE, sizeof(GOODBYE));
    close(sd);
  }   

  void addFile(string name) {
    int sd;
    sockaddr_in server;

    int key = this->SHA(name);
    pair<uint, uint> aux = this->findSucc(key);

    if(aux.second == this->myPort) {
      M[key] = name;
      return void(cout << "Am adaugat cu succes fisierul la nodul: " << this->poz << endl);
    }

    sd = socket(AF_INET, SOCK_STREAM, 0);
    if(sd == -1)
      return ;
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = htonl(INADDR_ANY);
    server.sin_port = htons(aux.second);
    if(connect(sd, (sockaddr*)&server, sizeof(sockaddr)) == -1)
      return ;

    int len = (int)name.size();
    write(sd, &ADDFILE, sizeof(ADDFILE));
    write(sd, &len, sizeof(len));
    for(auto it : name)
      write(sd, &it, sizeof(it));
    write(sd, &GOODBYE, sizeof(GOODBYE));
    close(sd);

    cout << "Am adaugat fisierul la nodul " << aux.first << endl;
  }

  void findFile(string name) {
    int sd;
    sockaddr_in server;

    uint key = this->SHA(name);
    pair<uint, uint> aux = this->findSucc(key);

    if(aux.second == this->myPort) {
      if(this->M.count(key))
        return void(cout << "Am gasit fisierul #" << M[key] << "# la nodul " << this->poz << endl);
      return void(cout << "Nu am putut gasi fisierul $" << name << "$. Imi pare rau :(" << endl);
    }

    sd = socket(AF_INET, SOCK_STREAM, 0);
    if(sd == -1)
      return ;
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = htonl(INADDR_ANY);
    if(aux.second == this->myPort) 
      return void(this->findFile(name));
    server.sin_port = htons(aux.second);
    if(connect(sd, (sockaddr*)&server, sizeof(sockaddr)) == -1)
      return ;

    int len = (int)name.size();
    int answer = 0;
    write(sd, &NEEDFILE, sizeof(NEEDFILE));
    write(sd, &len, sizeof(len));
    for(auto it : name)
      write(sd, &it, sizeof(it));
    read(sd, &answer, sizeof(answer));
    write(sd, &GOODBYE, sizeof(GOODBYE));
    close(sd);

    if(answer) 
      return void(cout << "Am gasit fisierul #" << name << "# la nodul " << aux.first << endl);
    return void(cout << "Nu am putut gasi fisierul $" << name << "$. Imi pare rau :(" << endl);
  }

  void nodeLeave() {
    if(this->succ.node == this->poz)
      return;

    int sd;
    sockaddr_in server;
    sd = socket(AF_INET, SOCK_STREAM, 0);
    if(sd == -1)
      return ;
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = htonl(INADDR_ANY);
    server.sin_port = htons(this->succ.port);
    if(connect(sd, (sockaddr*)&server, sizeof(sockaddr)) == -1)
      return ;

    for(auto elem : this->M) {
      write(sd, &ADDFILE, sizeof(ADDFILE));
      int len = elem.second.size();
      write(sd, &len, sizeof(len));
      for(auto it : elem.second)
        write(sd, &it, sizeof(it));
    }

    write(sd, &IQUIT, sizeof(IQUIT));
    pair<uint, uint> leaver = make_pair(this->poz, this->myPort);
    pair<uint, uint> replacer = make_pair(this->succ.node, this->succ.port);
    write(sd, &leaver, sizeof(leaver));
    write(sd, &replacer, sizeof(replacer));
    write(sd, &GOODBYE, sizeof(GOODBYE));
    close(sd);
  }

}me;

bool inRange(uint val, uint left, uint right){
  val += (1 << BITS); val &= MOD;
  left += (1 << BITS); left &= MOD;
  right += (1 << BITS); right &= MOD;

  if(left <= right && left <= val && val <= right) return 1;
  if(left > right && (val >= left || val <= right)) return 1;
  return 0;
}

void treat(int client) {
  while(1) {
    int op = -1;
    if(read(client, &op, sizeof(op)) < 0) {
      if(errno == 104) {
        sleep(2);
        continue;
      }
      return ;
    }

    if(op == GOODBYE) break;

    if(op == FINDSUCC) {
      int id;
      read(client, &id, sizeof(id));
      pair<uint, uint> aux = me.findSucc(id);
      write(client, &aux, sizeof(aux));
      continue;
    }

    if(op == GETPRED) {
      pair<uint, uint> aux = make_pair(me.pred, me.prport);
      write(client, &aux, sizeof(aux));
      continue;
    }

    if(op == GETSUCC) {
      pair<uint, uint> aux = make_pair(me.succ.node, me.succ.port);
      write(client, &aux, sizeof(aux));
      continue;
    }

    if(op == CHPRED) {
      pair<uint, uint> aux;
      read(client, &aux, sizeof(aux));
      if(inRange(aux.first, me.pred + 1, me.poz) && int(aux.first) != -1) {
        me.pred = aux.first;
        me.prport = aux.second;
      }
      continue;
    }

    if(op == GIVEFILE) {
      int left = 0, right = 0;
      read(client, &right, sizeof(right));
      read(client, &left, sizeof(left));
      vector<uint> v;

      for(auto it : me.M) {
        if(!inRange(it.first, left + 1, right))
          continue;
        v.push_back(it.first);
      }

      int len = (int)v.size();
      write(client, &len, sizeof(len));
      for(auto el : v) {
        write(client, &el, sizeof(el));
        string now = me.M[el];
        len = (int)now.size();
        write(client, &len, sizeof(len));
        for(auto it : now)
          write(client, &it, sizeof(it));
        me.M.erase(el);
      }
      continue;
    }

    if(op == GETCPN) {
      ;
      pair<uint, uint> aux;
      int id;
      read(client, &id, sizeof(id));
      aux = me.closestPrecedingNode(id);
      write(client, &aux, sizeof(aux));
      continue;
    }

    if(op == CHFINGER) {
      pair<uint, uint> aux;
      read(client, &aux, sizeof(aux));

      for(int i = 0; i < BITS; ++i) {
        if(!inRange(aux.first, me.poz, me.next[i].node - 1)) 
          continue;
        if(!inRange(aux.first, me.next[i].start, me.poz - 1))
          continue;

        me.next[i].node = aux.first;
        me.next[i].port = aux.second;
      }

      if(me.succ.port == me.myPort) continue;

      int sd;
      sockaddr_in server;
      sd = socket(AF_INET, SOCK_STREAM, 0);
      if(sd == -1)
        return ;
      server.sin_family = AF_INET;
      server.sin_addr.s_addr = htonl(INADDR_ANY);
      server.sin_port = htons(me.succ.port);
      if(connect(sd, (sockaddr*)&server, sizeof(sockaddr)) == -1)
        return ;

      write(sd, &CHFINGER, sizeof(CHFINGER));
      write(sd, &aux, sizeof(aux));
      write(sd, &GOODBYE, sizeof(GOODBYE));
      close(sd);

      continue;
    }

    if(op == NEEDFILE) {
      int len;
      char c;
      string name;

      read(client, &len, sizeof(len)) < 0;
      for(int i = 0; i < len ; ++i) {
        read(client, &c, sizeof(c));
        name += c;
      }

      int key = me.SHA(name);
      int answer = (me.M.count(key) > 0);
      write(client, &answer, sizeof(answer)) <= 0;
      continue;
    }

    if(op == ADDFILE) {
      int len;
      char c;
      string name;
      read(client, &len, sizeof(len));
      for(int i = 0; i < len; ++i) {
        read(client, &c, sizeof(c));
        name += c;
      }

      me.M[me.SHA(name)] = name;
    }

    if(op == IQUIT) {
      pair<uint, uint> leaver, replacer;
      read(client, &leaver, sizeof(leaver));
      read(client, &replacer, sizeof(replacer));

      for(int i = 0; i < BITS; ++i) {
        if(leaver.first != me.next[i].node)
          continue;

        me.next[i].node = replacer.first;
        me.next[i].port = replacer.second;
      }

      if(me.pred == leaver.first) {
        me.pred = replacer.first;
        me.prport = replacer.second;
      }

      if(me.succ.port == me.myPort)
        continue;

      int sd;
      sockaddr_in server;
      sd = socket(AF_INET, SOCK_STREAM, 0);
      if(sd == -1)
        return ;
      server.sin_family = AF_INET;
      server.sin_addr.s_addr = htonl(INADDR_ANY);
      server.sin_port = htons(me.succ.port);
      if(connect(sd, (sockaddr*)&server, sizeof(sockaddr)) == -1)
        return ;

      write(sd, &IQUIT, sizeof(IQUIT));
      write(sd, &leaver, sizeof(leaver));
      write(sd, &replacer, sizeof(replacer));
      write(sd, &GOODBYE, sizeof(GOODBYE));
      close(sd);
    }
  }

  close(client);
}

void serverThread() {
  sockaddr_in server, from;
  int sd, on = 1;

  sd = socket(AF_INET, SOCK_STREAM, 0);
  if(sd == -1)
      return ;

  setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

  bzero(&server, sizeof(server));
  bzero(&from, sizeof(from));

  server.sin_family = AF_INET;
  server.sin_addr.s_addr = htonl(INADDR_ANY);
  server.sin_port = htons(me.myPort);

  if(bind(sd, (sockaddr*)&server, sizeof(sockaddr)) == -1)
    return ;

  if(listen(sd, 63) == -1)
    return ;

  while(1) {
    int client;
    uint len = sizeof(from);

    client = accept(sd, (sockaddr*)&from, &len);
    if(client < 0)
      return ;

    thread (treat, client).join();
  }

  close(sd);
}

void fixTable() {
  while(1) {
    sleep(500);
    me.fixFinger();
    me.stabilize();
  }
}

int main() {
  me.init();

  while(1) {
    cout << "1) Find\n" << "2) Insert\n" << "3) Exit\n";
    cout << "Introduceti numarul functiei dorite: " << flush;
    cin >> op;
    
    if(op == 3) break;

    cout << "Introduceti denumirea fisierului: " << flush;
    cin >> name;

    if(op == 1) me.findFile(name);
    if(op == 2) me.addFile(name);
  }

  me.nodeLeave();

  return 0;
}
