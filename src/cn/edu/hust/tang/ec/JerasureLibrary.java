package cn.edu.hust.tang.ec;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public interface JerasureLibrary extends Library{
	JerasureLibrary INSTANCE = (JerasureLibrary) Native.loadLibrary(
			"Jerasure", JerasureLibrary.class);
	
	// Pointer -> int *   byte[] -> char *
	// multiply in GF (w=8)
	void galois_w08_region_multiply(byte[] region,      /* Region to multiply */
            int multby,       	/* Number to multiply by */
            int nbytes,        /* Number of bytes in region */
            byte[] r2,          /* If r2 != NULL, products go here */
            int add);
	
	// xor in GF
	void galois_region_xor(byte[] src, byte[] dest, int nbytes);
	
	// generate encode matrix
	Pointer reed_sol_vandermonde_coding_matrix(int k, int m, int w);
	
	// generate decode matrix
	// If erased[i] equals 0, then device i is working. If erased[i] equals 1, then it is erased
	int jerasure_make_decoding_matrix(int k, int m, int w, 
			Pointer matrix, int[] erased, int[] decoding_matrix, int[] dm_ids);
	
	// print matrix
	void jerasure_print_matrix(Pointer m, int rows, int cols, int w);
}
