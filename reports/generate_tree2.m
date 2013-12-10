%% Import top 50.
filename = 'top50.csv';
delimiter = {','};
formatSpec = '%*s%*s%s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%[^\n\r]';
fileID = fopen(filename,'r');
dataArray = textscan(fileID, formatSpec, 'Delimiter', delimiter,  'ReturnOnError', false);
fclose(fileID);
CRIME_NAMES = dataArray{:, 1};
clearvars filename delimiter formatSpec fileID dataArray ans;

%% Import most correlated.
filename = 'sorted_output.txt';
delimiter = {'\t'};
formatSpec = '%d%s%f%*[^\n\r]';
fileID = fopen(filename,'r');
dataArray = textscan(fileID, formatSpec, 'Delimiter', delimiter,  'ReturnOnError', false);
fclose(fileID);
TOP_CORRELATED_NAME = dataArray{:, 2};
TOP_CORRELATED = dataArray{:, 3};
clearvars filename delimiter formatSpec fileID dataArray ans;

%% find indicator names

file_list = dir('scatter/*');
indicators = containers.Map;
for i=3:length(file_list)
    name = file_list(i).name;
    name = name(9:end-10);
    
    if name(end) == 'C'
        name = name(1:end-1);
    end
    if name(1) ~= 'W'
        indicators(name(2:end)) = 1;
    end
end

indicators = keys(indicators);

%%

all_examples = zeros(1,length(indicators));
all_classes = cell(1,1);

crime_types={'BATTERY','SEX','ROBBERY'};
crime_types_explained={'BATTERY','SEXUAL ASSAULT','ROBBERY'};

for ct=1:length(crime_types)
    
    crime_type_indices = zeros(length(CRIME_NAMES),length(crime_types));
    
    %sprintf('Doing it for crimes of type %s\n',crime_types{ct})
    
    for i=1:length(CRIME_NAMES)
        %sprintf('Searching for %s ON %s',crime_types{ct},CRIME_NAMES{i})
        if ~isempty(strfind(CRIME_NAMES{i},crime_types{ct}))
            %sprintf('FOUND!\n')
            crime_type_indices(i,1) = 1;
        end
    end
    
    crime_indices = find(crime_type_indices(:,1)==1);
    [h w] = size(all_examples);
    start_index = h;
    for i=1:length(crime_indices)
        for ind=1:length(indicators)
            file_list = dir(sprintf('scatter/*%s*C%d-*',indicators{ind},crime_indices(i)));
            if length(file_list) >= 1
                filename = ['scatter/' file_list(1).name];
                %disp(filename);
                data=load(filename);
                sorted = sortrows(data,-2);
               
                [h w] = size(data);
                if h > 20
                    all_examples(start_index+i,ind) = mean(sorted(1:4,3));
                end
            end
        end
        all_classes{start_index+i} = crime_types_explained{ct};
    end
end

%indices_nan=isnan(all_examples);
sum2 = sum(all_examples(:,1:end),2);
%[h w] = size(all_examples);
all_examples = all_examples(sum2 ~= 0,:);


%%

plot(all_examples(find(all_examples(:,end)==1),2), 'ro');
hold on;
plot(all_examples(find(all_examples(:,end)==2),2), 'go');
plot(all_examples(find(all_examples(:,end)==3),2), 'bo');

%%

columns=[13 3 7 10];
examples = all_examples(:,columns);
classes = all_classes(2:end)';

%clear examples classes

%rnames = {'height','weight','spicy'};
%examples = [160 70 9; 150 80 10;150 56 10;180 100 7;210 100 3;200 80 1;160 50 4];
%classes = [1;1;1;1;2;2;2];

%t=classregtree(examples,classes,'method','classification');
tc = ClassificationTree.fit(examples,classes,'PredictorNames',indicators(columns),'minleaf',2);
view(tc,'mode','graph') 

