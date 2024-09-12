//Explanation:
//The fetchData() function triggers when the button is clicked. It sends a request to your 
//backend API endpoint (http://localhost:5000/api/getData) and fetches the data.
//The displayData(data) function takes the fetched data and displays it in an HTML table.

document.getElementById("fetchDataButton").addEventListener("click", fetchData);

function fetchData() {
  fetch('http://localhost:5000/api/getData')  // Replace with the correct backend API endpoint
    .then(response => response.json())
    .then(data => {
      displayData(data);
    })
    .catch(error => {
      console.error("Failed to fetch data:", error);
      document.getElementById("dataContainer").innerText = "Error fetching data.";
    });
}

function displayData(data) {
  const dataContainer = document.getElementById("dataContainer");
  dataContainer.innerHTML = ""; // Clear previous content

  if (data.length === 0) {
    dataContainer.innerText = "No data available.";
    return;
  }

  // Create a table to display data
  const table = document.createElement("table");
  const headerRow = document.createElement("tr");

  // Add table headers
  Object.keys(data[0]).forEach(key => {
    const th = document.createElement("th");
    th.innerText = key;
    headerRow.appendChild(th);
  });
  table.appendChild(headerRow);

  // Add data rows
  data.forEach(item => {
    const row = document.createElement("tr");
    Object.values(item).forEach(value => {
      const td = document.createElement("td");
      td.innerText = value;
      row.appendChild(td);
    });
    table.appendChild(row);
  });

  dataContainer.appendChild(table);
}
