#script to automatically login to DBS portal#

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException,StaleElementReferenceException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import time
import getpass
import os
from datetime import datetime

def switch_to_frame(driver, frame_element):
    # Switch to the specified frame, handling stale element reference exception
    tries = 3
    while tries > 0:
        try:
            driver.switch_to.frame(frame_element)
            return True
        except StaleElementReferenceException:
            tries -= 1
            time.sleep(1)
    return False

# Get the absolute path of the current script file
script_path = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the "Transactions_CSV" folder within your repository
directory_path = os.path.join(script_path, "Transactions_CSV")

# Create the "Transactions_CSV" folder if it doesn't exist
if not os.path.exists(directory_path):
    os.makedirs(directory_path)

# Configure Chrome options
chrome_options = webdriver.ChromeOptions()
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": directory_path
})

# Create a new instance of the Chrome driver with the configured options
driver = webdriver.Chrome(options=chrome_options)
driver.maximize_window()
time.sleep(3)
        
# Navigate to the webpage
driver.get("https://internet-banking.dbs.com.sg/IB/Welcome")

# Initialise wait period
wait =  WebDriverWait(driver, 20)

# ENTER CREDENTIALS #
# Locate the input text box by its HTML attribute
userID = driver.find_element(By.ID, "UID")
pin = driver.find_element(By.ID, "PIN")

# Clear any existing text in the text box
userID.clear()
pin.clear()

# Enter the respective credentials into the text box
# Retrieve credentials from user
userID_input = input("Enter your userID and click enter when you are done! \n")
userID.send_keys(userID_input)
pin_input = getpass.getpass("Enter your pin and click enter when you are done! (you will not be able to see the password you've typed!)\n")
pin.send_keys(pin_input) 

# LOGIN BUTTON #
# Locate the input text box by its HTML attribute
loginBtn = driver.find_element(By.XPATH, "//button[@class='btn btn-primary block mBot-12' and @title='Login']")

# Click the button
loginBtn.click()

# Add a wait of 7 seconds to ensure next page finishes loading
time.sleep(7)

# IN-APP AUTHENTICATION #
try:
    # Find all frame elements on the page 
    frame_elements = driver.find_elements(By.TAG_NAME, "frame")
    if frame_elements:
        print("Element exists")
        
        # Get all the names of all the frame elements
        frame_names = [frame.get_attribute("name") for frame in frame_elements]
        print("All frame names found:", frame_names)

        if "user_area" in frame_names:
            # Get the index and the name of the 'user_area' frame
            user_area_frame_index = frame_names.index("user_area")
            user_area_frame_name = frame_names[user_area_frame_index]
            print("Name attribute of 'user_area' frame:", user_area_frame_name)

            # Switch to the 'user_area' frame
            if switch_to_frame(driver, frame_elements[user_area_frame_index]):
                print("Switch success!")

                # Find all iframe elements within 'user_area' frame
                iframe_elements = driver.find_elements(By.TAG_NAME, "iframe")
                if iframe_elements:
                    print("IFRAME EXISTS")
                
                    # Get the names of all the iframe elements
                    desired_iframe_element = next((frame for frame in iframe_elements if frame.get_attribute("name")=="iframe1"), None)
                    if desired_iframe_element:
                        print("Desired iframe(iframe1) found")
                        
                        # Switch to the desired iframe
                        if switch_to_frame(driver, desired_iframe_element):
                            
                            # AUTHENTICATE BUTTON #
                            authenticateBtn = driver.find_element(By.ID, "AuthenticatBtnId")
                            
                            # Click the button
                            authenticateBtn.click()
                            
                            # Add a wait of 15 seconds to ensure in-app authentication is made manually
                            time.sleep(15)
                    
                            print("authentication done!")
                    
                            # Switch back to the default content outside the frame
                            # Switch back to User_Area Frame to access the summary tab
                            driver.switch_to.default_content()
                            switch_to_frame(driver,frame_elements[user_area_frame_index])
                    
                            # RETRIEVING TRANSACTION TAB #
                            accountSummaryTab = driver.find_element(By.ID, "topnav0")
                            accountSummaryTab.click() 
                    
                            transactionTab = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="topnav0"]/div[2]/a[10]')))
                            transactionTab.click()
                
                            # Switch back to the default content outside the frame
                            driver.switch_to.default_content()
                            
                            # Switch to the 'user_area' frame
                            driver.switch_to.frame(user_area_frame_name)
                            
                            # Find all iframe elements within 'user_area' frame again
                            iframe_elements_user_area = driver.find_elements(By.TAG_NAME, "iframe")
                            if iframe_elements_user_area:
                                print("IFRAME EXISTS within 'user_area' frame")

                                # Get the index of the desired iframe within 'user_area' frame
                                desired_iframe_index = next((index for index, frame in enumerate(iframe_elements_user_area) if frame.get_attribute("name") == "iframe1"), None)

                                if desired_iframe_index is not None:
                                    desired_iframe_element_user_area = iframe_elements_user_area[desired_iframe_index]

                                    # Switch to the desired iframe within the 'user_area' frame
                                    driver.switch_to.frame(desired_iframe_element_user_area)
                                    time.sleep(3)
                                    print("finding transaction period dd!")
                                    ddTransPeriod = wait.until(EC.element_to_be_clickable((By.ID, "selectRange")))
                                    ddTransPeriod.click()
                                    time.sleep(3)
                                    ddDateRange = wait.until(EC.element_to_be_clickable((By.ID, "specifyPeriod")))
                                    ddDateRange.click()
                                    
                                    ddTdyDate = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".today.active.start-date.available.in-range")))
                                    ddTdyDate.click()
                                    time.sleep(3)
                                    
                                    transactionGoBtn = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="search-more"]/div[1]/div[5]/button')))
                                    transactionGoBtn.click()
                                    time.sleep(3)
                                    
                                    transactionDownloadIcon = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/section/div/form/div/div/form/div[3]/div[2]/div/a[1]')))
                                    transactionDownloadIcon.click()
                                    
                                    def latest_download_file():
                                        path = directory_path
                                        os.chdir(path)
                                        files = sorted(os.listdir(os.getcwd()), key=os.path.getmtime)
                                        newest = files[-1]

                                        return newest

                                    fileends = "crdownload" #crdownload = Chrome Download
                                    while "crdownload" == fileends:
                                        time.sleep(1)
                                        newest_file = latest_download_file()
                                        if "crdownload" in newest_file:
                                            fileends = "crdownload"
                                        else:
                                            fileends = "none"
                                    
                                    # Rename the downloaded file to the desired format
                                    new_filename = "transactions_" + time.strftime("%d%m%y") + ".csv"
                                    new_filepath = os.path.join(directory_path, new_filename)
                                    os.rename(os.path.join(directory_path, newest_file), new_filepath)
                                
                                    print("Downloaded file renamed successfully!")

                                    #TEMPORARY!!! JUST USING FOR TESTING PURPOSES TO ENSURE BROWSER DOESNT CLOSE
                                    input("Press Enter to close the browser...")
                        else:
                            print("failed to switch the desired iframe")
                       
                    else:
                        print("Desired iframe does not exist")

                # Switch back to the default content outside the frames
                driver.switch_to.default_content()

            else:
                print("No iframes found")

        else:
            print("Frame 'user_area' does not exist")

    else:
        print("Frame Element does not exist")

except NoSuchElementException:
    print("Element does not exist")