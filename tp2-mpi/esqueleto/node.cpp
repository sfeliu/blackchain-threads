#include "node.h"
#include "picosha2.h"
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <cstdlib>
#include <queue>
#include <atomic>
#include <mpi.h>
#include <map>

int total_nodes, mpi_rank;
Block *last_block_in_chain;
map<string,Block> node_blocks;

// Lockear este mutes cuando se quiere enviar un bloque nuevo
// Intentar Unlockear este mutex cuando se necesita procesar un bloque recibido
// No se me ocurrió un mejor nombre para este mutex...
pthread_mutex_t procesando_bloque = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t usando_last_block = PTHREAD_MUTEX_INITIALIZER;

//Cuando me llega una cadena adelantada, y tengo que pedir los nodos que me faltan
//Si nos separan más de VALIDATION_BLOCKS bloques de distancia entre las cadenas, se descarta por seguridad
bool verificar_y_migrar_cadena(const Block *rBlock, const MPI_Status *status){
    MPI_Request request;
    MPI_Status local_status;
    
    //TODO: Enviar mensaje TAG_CHAIN_HASH
    MPI_Isend((void *)rBlock->block_hash,
              HASH_SIZE,
              MPI_CHAR,
              status->MPI_SOURCE,
              TAG_CHAIN_HASH,
              MPI_COMM_WORLD,
              &request);
    
    MPI_Wait(&request, &local_status);

    Block *blockchain = new Block[VALIDATION_BLOCKS];

    //TODO: Recibir mensaje TAG_CHAIN_RESPONSE
    MPI_Recv((void *) blockchain,
             VALIDATION_BLOCKS,
             *MPI_BLOCK,
             status->MPI_SOURCE,
             TAG_CHAIN_RESPONSE,
             MPI_COMM_WORLD,
             &local_status);
    int bloques_recibidos;
    MPI_Get_count(&local_status, *MPI_BLOCK, &bloques_recibidos);
    
    //TODO: Verificar que los bloques recibidos
    //sean válidos y se puedan acoplar a la cadena
    // Verifico que el primero en lacadena recibida es el rBlock (esto no me queda claro del enunciado, preguntar!)
    if (strncmp(blockchain[0].block_hash, rBlock->block_hash, HASH_SIZE) != 0) {
        cout << "El primer bloque enviado no es el original, ignorar la cadena recibida" << endl;
        delete []blockchain;
        return false;
    }

    if (blockchain[0].index != rBlock->index) {
        cout << "El índice del primer bloque no coincide con el esperado" << endl;
        delete []blockchain;
        return false;
    }
    
    // Verifico que el hash del primer elemento de la cadena es correcto
    string hash_result;
    block_to_hash(&blockchain[0], hash_result); // porque devolver una string en vez de un char[HASH_SIZE]?
    if (hash_result.compare(0, string::npos, blockchain[0].block_hash, HASH_SIZE) != 0) {
        cout << "El hash del primer bloque no es correcto" << endl;
    }

    for (int i = 1; i < bloques_recibidos; i++) {
        if (strncmp(blockchain[i].block_hash, blockchain[i-1].previous_block_hash, HASH_SIZE) != 0) {
            cout << "El previous_hash del nodo " << i << "no es igual al hash_block del nodo " << i-1;
            cout << "La cadena recibida esta rota, la ignoro" << endl;
            delete []blockchain;
            return false;
        }
        
        if (blockchain[i-1].index-1 != blockchain[i].index) {
            cout << "Indices incorrectos en la cadena recibida" << endl;
            delete []blockchain;
            return false;
        }

        if (node_blocks.count(string(blockchain[i].block_hash, HASH_SIZE)) != 0) {
            // Acepto la cadena
            *last_block_in_chain = *rBlock;
            for (int j = 1; j < i; j++) {
                node_blocks[string(blockchain[j].block_hash, HASH_SIZE)] = blockchain[j];
            }
            
            delete []blockchain;
            return true;
        }

    }

    // La cadena no tiene errores, pero no se pudieron encontrar bloques en node_blocks.
    // Si el último bloque de la cadena recibida es de index 1, la acepto, si no, la descarto
    if (blockchain[bloques_recibidos-1].index == 1) {
        *last_block_in_chain = *rBlock;
        delete []blockchain;
        return true;
    }
    
    delete []blockchain;
    return false;
}

//Verifica que el bloque tenga que ser incluido en la cadena, y lo agrega si corresponde
bool validate_block_for_chain(const Block *rBlock, const MPI_Status *status){
  if(valid_new_block(rBlock)) {
      // SF: Deprecated (?)
      // Quizás use last_block, aviso así los demás threads para evitar condiciones de carrera
      //pthread_mutex_lock(&usando_last_block);

      //Agrego el bloque al diccionario, aunque no
      //necesariamente eso lo agrega a la cadena
      //node_blocks[string(rBlock->block_hash)] = *rBlock;

      //TODO: Si el índice del bloque recibido es 1
      //y mí último bloque actual tiene índice 0,
      //entonces lo agrego como nuevo último.
      if (rBlock->index == 1 && last_block_in_chain->index == 0) {
          // Bastaría con copiar el puntero?
          *last_block_in_chain = *rBlock;
          printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index,
                 status->MPI_SOURCE);
          return true;
      }

      //TODO: Si el índice del bloque recibido es
      //el siguiente a mí último bloque actual,
      //y el bloque anterior apuntado por el recibido es mí último actual,
      //entonces lo agrego como nuevo último.
      if (rBlock->index == last_block_in_chain->index + 1
          && rBlock->previous_block_hash == last_block_in_chain->block_hash) {

          // Bastaría con copiar el puntero?
          *last_block_in_chain = *rBlock;
          printf("[%d] Agregado a la lista bloque con index %d enviado por %d \n", mpi_rank, rBlock->index,
                 status->MPI_SOURCE);
          return true;
      }

      //TODO: Si el índice del bloque recibido es
      //el siguiente a mí último bloque actual,
      //pero el bloque anterior apuntado por el recibido no es mí último actual,
      //entonces hay una blockchain más larga que la mía.
      if (rBlock->index == last_block_in_chain->index + 1
          && rBlock->previous_block_hash != last_block_in_chain->block_hash) {

          printf("[%d] Perdí la carrera por uno (%d) contra %d \n", mpi_rank, rBlock->index, status->MPI_SOURCE);
          bool res = verificar_y_migrar_cadena(rBlock, status);
          return res;
      }

      //TODO: Si el índice del bloque recibido es igual al índice de mi último bloque actual,
      //entonces hay dos posibles forks de la blockchain pero mantengo la mía
      if (rBlock->index == last_block_in_chain->index) {
          printf("[%d] Conflicto suave: Conflicto de branch (%d) contra %d \n", mpi_rank, rBlock->index,
                 status->MPI_SOURCE);
          return false;
      }

      // SF: No lo encuentro en el enunciado. Si alguien me manda un screenshot lo agradezco.
      //TODO: Si el índice del bloque recibido es anterior al índice de mi último bloque actual,
      //entonces lo descarto porque asumo que mi cadena es la que está quedando preservada.
      if (rBlock->index < last_block_in_chain->index) {
          printf("[%d] Conflicto suave: Descarto el bloque (%d vs %d) contra %d \n", mpi_rank, rBlock->index,
                 last_block_in_chain->index, status->MPI_SOURCE);
          return false;
      }

      //TODO: Si el índice del bloque recibido está más de una posición adelantada a mi último bloque actual,
      //entonces me conviene abandonar mi blockchain actual
      if (rBlock->index > last_block_in_chain->index) {
          printf("[%d] Perdí la carrera por varios contra %d \n", mpi_rank, status->MPI_SOURCE);
          bool res = verificar_y_migrar_cadena(rBlock, status);
          return res;
      }

      //pthread_mutex_unlock(&usando_last_block);
  }

  printf("[%d] Error duro: Descarto el bloque recibido de %d porque no es válido \n",mpi_rank,status->MPI_SOURCE);
  return false;
}

// Envio los bloques siguiendo los ranks. Me asegura que para cada programa, la 
// secuencia es distinta
void broadcast_block(const Block *block){
    // Avisar que estoy mandando un bloque nuevo
    // pthread_mutex_lock(&procesando_bloque);
    // SF: Lo puse fuera, para que no haya condiciones de carrera

    int rank; // mi id en MPI_COMM_WORLD
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    int np; // numero de procesos en MPI_COMM_WORLD
    MPI_Comm_size(MPI_COMM_WORLD, &np);

    MPI_Status status[np-1];
    MPI_Request requests[np-1];
    
    int i = 0; // indice de requests y status
    for (int dst = (rank+1) % np; dst != rank; dst = (dst + 1) % np) {
        MPI_Isend((void *)block, 1, *MPI_BLOCK, dst, TAG_NEW_BLOCK, MPI_COMM_WORLD, &requests[i]);
        cout << mpi_rank << " envio bloque a " << dst << endl;
        i++;
    }
    MPI_Waitall(np-1, requests, status);

    // Ya mandé el bloque, suelto el mutex
    pthread_mutex_unlock(&procesando_bloque);
}

//Proof of work
//TODO: Advertencia: puede tener condiciones de carrera
void* proof_of_work(void *ptr){
    string hash_hex_str;
    Block block;
    unsigned int mined_blocks = 0;
    while(true){
        // No quiero que el otro thread cambie last_block_in_chain mientras intento crear un nuevo bloque que apunta a el?
        // quizás esto bloquea demasiado?
        // SF: Creo que esto lo resuelve el mutex procesando_bloque
        //pthread_mutex_lock(&usando_last_block);
        block = *last_block_in_chain;

        //Preparar nuevo bloque
        block.index += 1;
        block.node_owner_number = mpi_rank;
        block.difficulty = DEFAULT_DIFFICULTY;
        block.created_at = static_cast<unsigned long int> (time(NULL));
        memcpy(block.previous_block_hash,block.block_hash,HASH_SIZE);

        // SF: Esto no deberia hacerse hasta que se logre el resultado? de la siguiente manera?

        //Agregar un nonce al azar al bloque para intentar resolver el problema
        gen_random_nonce(block.nonce);

        //Hashear el contenido (con el nuevo nonce)
        block_to_hash(&block,hash_hex_str);

        // SF: De esta manera.
        while(solves_problem(hash_hex_str)){
            gen_random_nonce(block.nonce);
            block_to_hash(&block,hash_hex_str);
        }

        // Avisar que voy a agregar un bloque, no recibir mensajes.
        pthread_mutex_lock(&procesando_bloque);

        //Verifico que no haya cambiado mientras calculaba/esperaba el mensaje
        if(last_block_in_chain->index < block.index) {
            mined_blocks += 1;
            *last_block_in_chain = block;
            strcpy(last_block_in_chain->block_hash, hash_hex_str.c_str());
            node_blocks[hash_hex_str] = *last_block_in_chain;
            printf("[%d] Agregué un producido con index %d \n", mpi_rank, last_block_in_chain->index);

            //TODO: Mientras comunico, no responder mensajes de nuevos nodos
             broadcast_block(last_block_in_chain);
            }
        //pthread_mutex_unlock(&usando_last_block);

        // El loop termina al obtener una blockchain de tamaño maximo.
        if(last_block_in_chain->index == MAX_BLOCKS){
            break;
        }
    }

    return NULL;
}

void enviar_bloques(Block *rBlock, const MPI_Status *status){
    Block *blockchain = new Block[VALIDATION_BLOCKS];
    Block *lastBlock = rBlock;

    int i = 0;
    while(i < VALIDATION_BLOCKS){
        if(i > 0 && lastBlock->index == 0)break;
        blockchain[i] = *lastBlock;
        lastBlock = &node_blocks.find(lastBlock->previous_block_hash)->second;
        i++;
    }

    MPI_Send((void *) blockchain, i, *MPI_BLOCK, status->MPI_SOURCE, TAG_CHAIN_RESPONSE, MPI_COMM_WORLD);

    delete []blockchain;
}

int node(){

  //Tomar valor de mpi_rank y de nodos totales
  MPI_Comm_size(MPI_COMM_WORLD, &total_nodes);
  MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

  //La semilla de las funciones aleatorias depende del mpi_ranking
  srand(time(NULL) + mpi_rank);
  printf("[MPI] Lanzando proceso %u\n", mpi_rank);

  last_block_in_chain = new Block;

  //Inicializo el primer bloque
  last_block_in_chain->index = 0;
  last_block_in_chain->node_owner_number = mpi_rank;
  last_block_in_chain->difficulty = DEFAULT_DIFFICULTY;
  last_block_in_chain->created_at = static_cast<unsigned long int> (time(NULL));
  memset(last_block_in_chain->previous_block_hash,0,HASH_SIZE);

  //TODO: Crear thread para minar
  int rc;
  pthread_attr_t attr;
  pthread_t thread;

  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

  rc = pthread_create(&thread, &attr, proof_of_work, NULL);
  if(rc){
    printf("ERROR; return code from pthread_create() is %d\n", rc);
    exit(-1);
  }

  //Esto lo creo para el mensaje entrante
  int flag;
  Block new_block;
  MPI_Request request;
  MPI_Status status;

  while(true){

      //TODO: Recibir mensajes de otros nodos

      //Antes de esto faltaria chequear que no se este enviando ningun bloque nuevo
      MPI_Irecv(&new_block, 1, *MPI_BLOCK, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &request);
      MPI_Test(&request, &flag, &status);

      // Aviso que estoy procesando un bloque recibido
      // Según lo que entendí del enunciado, no se puede procesar bloques recibidos y
      // enviar bloques nuevos de manera concurrente
      // SF: Correcto
      pthread_mutex_lock(&procesando_bloque);

      if(flag != 0 && status.MPI_SOURCE != mpi_rank){

        //TODO: Si es un mensaje de nuevo bloque, llamar a la función
        //validate_block_for_chain con el bloque recibido y el estado de MPI
        if(status.MPI_TAG == TAG_NEW_BLOCK){
          const Block rBlock = new_block;
          validate_block_for_chain(&rBlock, &status);
        }

        //TODO: Si es un mensaje de pedido de cadena,
        //responderlo enviando los bloques correspondientes
        else if(status.MPI_TAG == TAG_CHAIN_HASH) {
            enviar_bloques(&new_block, &status);
        }
      }

      // Terminado de procesar bloque recibido, soltar el mutex
      pthread_mutex_unlock(&procesando_bloque);

      // El loop termina al obtener una blockchain de tamaño maximo.
      if(last_block_in_chain->index == MAX_BLOCKS){
          break;
      }
  }

  pthread_attr_destroy(&attr);

  rc = pthread_join(thread, nullptr);
  if(rc){
    printf("ERROR; return code from pthread_join() is %d\n", rc);
    exit(-1);
  }

  delete last_block_in_chain;
  return 0;
}
