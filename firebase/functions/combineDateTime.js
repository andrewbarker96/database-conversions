// Helper function to convert time to 24-hour format and combine with date
export function combineDateTime(date, time) {
  if (!date || !time) {
    console.warn("Missing date or time:", { date, time });
    return null;
  }

  try {
    // Ensure the date is formatted as YYYY-MM-DD
    const [month, day, year] = date.split("-");
    const formattedDate = `${year}-${month.padStart(2, "0")}-${day.padStart(
      2,
      "0"
    )}`;

    // Convert time to 24-hour format
    const [hour, minute, secondPart] = time.split(":");
    const [second, period] = secondPart.split(" ");
    let hour24 = parseInt(hour, 10);

    if (isNaN(hour24) || isNaN(minute) || isNaN(second)) {
      console.warn("Invalid time components:", { date, time });
      return null;
    }

    if (period.toUpperCase() === "PM" && hour24 !== 12) hour24 += 12;
    if (period.toUpperCase() === "AM" && hour24 === 12) hour24 = 0;

    const isoString = new Date(
      `${formattedDate}T${String(hour24).padStart(2, "0")}:${minute}:${second}`
    ).toISOString();

    return isoString;
  } catch (error) {
    console.error("Error combining date and time:", {
      date,
      time,
      error,
    });
    return null;
  }
}
