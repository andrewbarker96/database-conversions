// Import required modules and SDKs
import { initializeApp } from "firebase/app";
import { collection, getDocs, getFirestore } from "firebase/firestore";
import { createClient } from "@supabase/supabase-js";
import { config } from "dotenv";
import fs from "fs";
import { exit } from "process";

// Load environment variables for Supabase
config();

// Firebase configuration
const firebaseConfig = {
  apiKey: "AIzaSyCGc9ieWeevCMiMPBDv3VEYuedyK2Xe2jI",
  authDomain: "stockassoc-a0091.firebaseapp.com",
  databaseURL: "https://stockassoc-a0091-default-rtdb.firebaseio.com",
  projectId: "stockassoc-a0091",
  storageBucket: "stockassoc-a0091.appspot.com",
  messagingSenderId: "1080703289236",
  appId: "1:1080703289236:web:a13f5bec71795e62083181",
  measurementId: "G-3MHPJHP7BR",
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);
const db = getFirestore(app);

// Supabase configuration
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;

// Initialize Supabase client
const supabase = createClient(supabaseUrl, supabaseKey);
 
async function fetchFirebaseData() {
  try {
    let allData = [];

    // Fetch data from Firebase Firestore
    const querySnapshot = await getDocs(collection(db, "guests"));
    querySnapshot.forEach((doc) => {
      const firebaseData = doc.data();
      firebaseData.id = doc.id; // Add document ID to the data
      allData.push(firebaseData);
    });

    // Convert the fetched data to JSON and write to file
    const jsonData = JSON.stringify(allData, null, 2);
    fs.writeFileSync("data.json", jsonData);
    console.log('"data.json" has been created');

    // Return the data for further processing
    return allData;
  } catch (err) {
    console.error("Error fetching data from Firebase:", err);
    throw err;
  }
}

async function dataToSupabase(data) {
  try {
    // Insert or update data into Supabase
    const { data: visitors, error } = await supabase
      .from("signin_app_visitors")
      .upsert(data, { onConflict: ["id"] });
    if (error) {
      console.error("Error upserting data to Supabase:", error);
    } else {
      console.log("Data upserted successfully to Supabase:", visitors);
    }
  } catch (err) {
    console.error("Error in Supabase upsert operation:", err);
  }
}

async function main() {
  try {
    // Step 1: Fetch data from Firebase and write to JSON
    const firebaseData = await fetchFirebaseData();

    // Step 2: Upsert the data into Supabase
    await dataToSupabase(firebaseData);
    console.log("Process Completed Successfully");
    exit(0);
  } catch (err) {
    console.error("Error in the main workflow:", err);
    exit(1);
  }
}

// Execute the main function
main();
