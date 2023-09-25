#include <cryptopp/cryptlib.h>
#include <cryptopp/osrng.h>
#include <cryptopp/ccm.h>
#include <cryptopp/gcm.h>
#include <iostream>
#include <iomanip>
#include <string>
#include <stdio.h>

#include <random>

const std::int32_t BLOCK_SIZE = 4096;
const std::int32_t ITERATION = 1000;

void getPseudoRandomFromCppMt19937Generator(std::vector<uint8_t> &entropy_vector, const size_t &entropy_size)
{
    std::random_device rd;
    std::seed_seq seedSeq{rd(), rd(), rd(), rd(), rd(), rd(), rd()};
    std::mt19937 gen(seedSeq);
    std::uniform_int_distribution<uint8_t> uchardist(0U, UCHAR_MAX);

    entropy_vector.reserve(entropy_size);
    std::generate_n(std::back_inserter(entropy_vector), entropy_size, [&gen, &uchardist] { return uchardist(gen); });
}


static inline uint64_t
rte_rdtsc(void)
{
        uint64_t tsc;

        asm volatile("mrs %0, pmccntr_el0" : "=r"(tsc));
        return tsc;
}

void testCCMcpuCycles()
{
    CryptoPP::AutoSeededRandomPool prng;
    std::vector<std::uint8_t> entropy_vector;

    getPseudoRandomFromCppMt19937Generator(entropy_vector, BLOCK_SIZE*ITERATION);

    CryptoPP::SecByteBlock key( CryptoPP::AES::DEFAULT_KEYLENGTH );
    prng.GenerateBlock( key, key.size() );

    unsigned char iv[ 12 ];
    prng.GenerateBlock( iv, sizeof(iv) );

    const int TAG_SIZE = 8;

    // Plain text
    std::string pdata;

    // Encrypted, with Tag
    std::string cipher;

    try
    {
        std::cout << "=======================================================================" << std::endl;
        std::cout << "|                                CCM TEST                             |" << std::endl;
        std::cout << "=======================================================================" << std::endl << std::endl;

        pdata.reserve(BLOCK_SIZE);
        for(auto i = 0; i < ITERATION; ++i){

            CryptoPP::CCM< CryptoPP::AES, TAG_SIZE >::Encryption e;
            e.SetKeyWithIV( key, key.size(), iv, sizeof(iv) );

            pdata.assign( entropy_vector.begin() + i*BLOCK_SIZE, entropy_vector.begin() + (i+1)*BLOCK_SIZE );

            e.SpecifyDataLengths( 0, pdata.size(), 0 );

            const std::int64_t start = rte_rdtsc();
            CryptoPP::StringSource ss1( pdata, true,
                new CryptoPP::AuthenticatedEncryptionFilter( e,
                    new CryptoPP::StringSink( cipher )
                ) // AuthenticatedEncryptionFilter
            ); // StringSource
            const std::int64_t stop = rte_rdtsc();

        std::cout << "start: " << start << " stop: " << stop << " diff: " << stop - start << " per byte: " << (stop - start)/BLOCK_SIZE << std::endl;
        }
        std::cout << "=======================================================================" << std::endl << std::endl;

    }
    catch( CryptoPP::Exception& e )
    {
        std::cerr << "Caught Exception..." << std::endl;
        std::cerr << e.what() << std::endl;
        std::cerr << std::endl;
    }
}

void testGCMcpuCycles()
{
    CryptoPP::AutoSeededRandomPool prng;
    std::vector<std::uint8_t> entropy_vector;

    getPseudoRandomFromCppMt19937Generator(entropy_vector, BLOCK_SIZE*ITERATION);

    CryptoPP::SecByteBlock key( CryptoPP::AES::DEFAULT_KEYLENGTH );
    prng.GenerateBlock( key, key.size() );

    unsigned char iv[ CryptoPP::AES::BLOCKSIZE ];
    prng.GenerateBlock( iv, sizeof(iv) );

    const int TAG_SIZE = 12;

    // Plain text
    std::string pdata;
    // Encrypted, with Tag
    std::string cipher;

    try
    {

        std::cout << "=======================================================================" << std::endl;
        std::cout << "|                                GCM TEST                             |" << std::endl;
        std::cout << "=======================================================================" << std::endl << std::endl;

        pdata.reserve(BLOCK_SIZE);
        for(auto i = 0; i < ITERATION; ++i){

            //CryptoPP::GCM< CryptoPP::AES >::Encryption e;
            CryptoPP::GCM< CryptoPP::AES, CryptoPP::GCM_64K_Tables >::Encryption e;
            e.SetKeyWithIV( key, key.size(), iv, sizeof(iv) );

            pdata.assign( entropy_vector.begin() + i*BLOCK_SIZE, entropy_vector.begin() + (i+1)*BLOCK_SIZE );

            const std::int64_t start = rte_rdtsc();
        CryptoPP::StringSource ss1( pdata, true,
                new CryptoPP::AuthenticatedEncryptionFilter( e,
                    new CryptoPP::StringSink( cipher ), false, TAG_SIZE
                ) // AuthenticatedEncryptionFilter
            ); // StringSource
            const std::int64_t stop = rte_rdtsc();


            std::cout << "start: " << start << " stop: " << stop << " diff: " << stop - start << " per byte: " << (stop - start)/BLOCK_SIZE << std::endl;
        }
        std::cout << "=======================================================================" << std::endl << std::endl;

    }
    catch( CryptoPP::Exception& e )
    {
        std::cerr << e.what() << std::endl;
        std::exit(1);
    }
}


int main(){

    testCCMcpuCycles();
    testGCMcpuCycles();

    return 0;
}
